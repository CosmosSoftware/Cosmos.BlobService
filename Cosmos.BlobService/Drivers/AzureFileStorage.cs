using Amazon.Auth.AccessControlPolicy;
using Azure.Storage.Files.Shares;
using Azure.Storage.Files.Shares.Models;
using Cosmos.BlobService.Config;
using Cosmos.BlobService.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Cosmos.BlobService.Drivers
{
    /// <summary>
    /// Driver for Azure File Share service
    /// </summary>
    public class AzureFileStorage : ICosmosStorage
    {
        // See: https://github.com/Azure/azure-sdk-for-net/blob/main/sdk/storage/Azure.Storage.Files.Shares/samples/Sample01b_HelloWorldAsync.cs
        private readonly ShareClient _shareClient;

        /// <summary>
        ///     Constructor
        /// </summary>
        /// <param name="config"></param>
        /// <param name="shareName">File share name</param>
        public AzureFileStorage(AzureStorageConfig config)
        {
            _shareClient = new ShareClient(config.AzureBlobStorageConnectionString, config.AzureFileShare);
            _ = _shareClient.CreateIfNotExistsAsync().Result;
        }

        /// <inheritdoc/>
        public async Task AppendBlobAsync(byte[] data, FileUploadMetaData fileMetaData, DateTimeOffset uploadDateTime)
        {
            // Name of the directory and file we'll create
            var dirName = Path.GetDirectoryName(fileMetaData.RelativePath);
            // Get a reference to a directory and create it

            var directory = _shareClient.GetDirectoryClient(dirName);
            if (!await directory.ExistsAsync())
            {
                await CreateFolderAsync(dirName); // Make sure the folder exists.
            }
            // Get a reference to a file and upload it
            ShareFileClient file = directory.GetFileClient(fileMetaData.FileName);
            using var memStream = new MemoryStream(data);

            if (!await file.ExistsAsync())
            {
                await file.CreateAsync(fileMetaData.TotalFileSize);
            }
            await file.UploadAsync(memStream);

        }

        /// <inheritdoc/>
        public async Task<bool> BlobExistsAsync(string path)
        {
            // Name of the directory and file we'll create
            var dirName = Path.GetDirectoryName(path);
            // Get a reference to a directory and create it
            var directory = _shareClient.GetDirectoryClient(dirName);
            if (!await directory.ExistsAsync())
            {
                return false;
            }

            var fileName = Path.GetFileName(path);
            var file = directory.GetFileClient(fileName);
            
            return await file.ExistsAsync();
        }

        /// <summary>
        /// Rename a file
        /// </summary>
        /// <param name="target"></param>
        /// <param name="newName"></param>
        /// <returns></returns>
        public async Task Rename(string target, string newFileName)
        {
            var sourceDirName = Path.GetDirectoryName(target.TrimStart('/'));
            var originalName = Path.GetFileName(target);
            var sourceDirectory = _shareClient.GetDirectoryClient(sourceDirName);
            var file = sourceDirectory.GetFileClient(originalName);
            await file.RenameAsync(newFileName);
        }

        /// <inheritdoc/>
        public async Task CopyBlobAsync(string source, string destination)
        {
            // Name of the directory and file we'll create
            var sourceDirName = Path.GetDirectoryName(source.TrimStart('/'));
            var sourceDirectory = _shareClient.GetDirectoryClient(sourceDirName);
            if (!await sourceDirectory.ExistsAsync())
            {
                throw new Exception($"Source directory not found: {sourceDirName}");
            }

            // Name of the directory and file we'll create
            var destDirName = Path.GetDirectoryName(destination.TrimStart('/'));
            var destDirectory = _shareClient.GetDirectoryClient(destDirName);

            if (!await destDirectory.ExistsAsync())
            {
                await CreateFolderAsync(destDirName);
            }

            var fileName = Path.GetFileName(source);
            var sourceFile = sourceDirectory.GetFileClient(fileName);
            if (!await sourceFile.ExistsAsync())
            {
                throw new Exception($"File not found: {fileName}");
            }
            var info = await sourceFile.GetPropertiesAsync();

            var destFile = destDirectory.GetFileClient(fileName);
            await destFile.DeleteIfExistsAsync();
            await destFile.CreateAsync(info.Value.ContentLength);

            using var readStream = await sourceFile.OpenReadAsync();
            await destFile.UploadAsync(readStream);

        }

        /// <inheritdoc/>
        public async Task CreateFolderAsync(string target)
        {
            // Name of the directory and file we'll create
            target = target.TrimStart('/');

            var pathParts = target.Split('/').ToList();

            var path = "";
            for (var i = 0; i < pathParts.Count; i++)
            {
                if (i == 0)
                {
                    path = "/" + pathParts[i];
                }
                else
                {
                    path = path + "/" + pathParts[i];
                }

                var directory = _shareClient.GetDirectoryClient(path);
                await directory.CreateIfNotExistsAsync();
            }
        }

        /// <inheritdoc/>
        public async Task<int> DeleteFolderAsync(string target)
        {
            var rootDir = _shareClient.GetRootDirectoryClient();
            var folder = rootDir.GetSubdirectoryClient(target.TrimEnd('/'));
            await DeleteAllAsync(folder);

            return 0;
        }

        private async Task DeleteAllAsync(ShareDirectoryClient dirClient)
        {

            await foreach (ShareFileItem item in dirClient.GetFilesAndDirectoriesAsync())
            {
                if (item.IsDirectory)
                {
                    var subDir = dirClient.GetSubdirectoryClient(item.Name);
                    await DeleteAllAsync(subDir);
                }
                else
                {
                    await dirClient.DeleteFileAsync(item.Name);
                }
            }

            await dirClient.DeleteAsync();
        }

        /// <inheritdoc/>
        public async Task DeleteIfExistsAsync(string target)
        {
            // Name of the directory and file we'll create
            var dirName = Path.GetDirectoryName(target);
            var fileName = Path.GetFileName(target);
            if (string.IsNullOrEmpty(fileName))
            {
                return;  // This is a directory, do nothing.
            }
            // Get a reference to a directory and create it
            var directory = _shareClient.GetDirectoryClient(dirName);
            var file = directory.GetFileClient(fileName);
            await file.DeleteIfExistsAsync();
        }

        /// <inheritdoc/>
        public async Task<List<string>> GetBlobNamesByPath(string path, string[] filter = null)
        {
            var list = new List<string>();

            // Name of the directory and file we'll create
            var dirName = Path.GetDirectoryName(path);
            // Get a reference to a directory and create it
            var directory = _shareClient.GetDirectoryClient(dirName);
            var contents = directory.GetFilesAndDirectoriesAsync();

            int i = 0;
            await foreach (var item in contents)
            {
                if (item.IsDirectory)
                {
                    var subDirectory = directory.GetSubdirectoryClient(item.Name);
                    var results = await GetAllBlobsAsync(subDirectory);
                    list.AddRange(results);
                }
                else
                {
                    list.Add($"{path}/{item.Name}");
                }
            }

            return list;
        }

        /// <summary>
        /// Gets all the files in a folder (recursive)
        /// </summary>
        /// <param name="dirClient"></param>
        /// <returns></returns>
        private async Task<List<string>> GetAllBlobsAsync(ShareDirectoryClient dirClient)
        {
            var list = new List<string>();

            await foreach (ShareFileItem item in dirClient.GetFilesAndDirectoriesAsync())
            {
                if (item.IsDirectory)
                {
                    var subDir = dirClient.GetSubdirectoryClient(item.Name);
                    list.AddRange(await GetAllBlobsAsync(subDir));
                }
                else
                {
                    var path = dirClient.Path.Replace("\\", "/");
                    list.Add($"{path}/{item.Name}");
                }
            }

            return list;
        }

        /// <inheritdoc/>
        public async Task<FileMetadata> GetFileMetadataAsync(string target)
        {

            // Name of the directory and file we'll create
            var dirName = Path.GetDirectoryName(target);
            var fileName = Path.GetFileName(target);
            if (string.IsNullOrEmpty(fileName))
            {
                return null;  // This is a directory, do nothing.
            }
            // Get a reference to a directory and create it
            var directory = _shareClient.GetDirectoryClient(dirName);
            var file = directory.GetFileClient(fileName);
            var props = await file.GetPropertiesAsync();

            return new FileMetadata()
            {
                ContentLength = props.Value.ContentLength,
                ContentType = props.Value.ContentType,
                ETag = props.Value.ETag.ToString(),
                FileName = file.Name,
                LastModified = props.Value.LastModified,
                UploadDateTime = props.Value.LastModified.UtcDateTime.Ticks
            };
        }


        /// <inheritdoc/>
        public Task<List<FileMetadata>> GetInventory()
        {
            throw new NotImplementedException();
        }


        /// <summary>
        /// Gets a blob object
        /// </summary>
        /// <param name="target"></param>
        /// <returns></returns>
        public async Task<FileManagerEntry> GetBlobAsync(string target)
        {
            if (string.IsNullOrEmpty(target)) return null;

            target = target.TrimStart('/');

            var dirName = Path.GetDirectoryName(target);
            var fileName = Path.GetFileName(target);

            var directory = _shareClient.GetDirectoryClient(dirName);

            if (!await directory.ExistsAsync())
            {
                return null;
            }

            var file = directory.GetFileClient(fileName);

            if (!await file.ExistsAsync())
            {
                return null;
            }

            var info = await file.GetPropertiesAsync();

            DateTime dateTime;
            if (info.Value.Metadata.TryGetValue("ccmsdatetime", out string uploaded))
            {
                dateTime = DateTimeOffset.Parse(uploaded).UtcDateTime;
            }
            else
            {
                dateTime = info.Value.LastModified.UtcDateTime;
            }

            return new FileManagerEntry()
            {
                Created = dateTime,
                CreatedUtc = dateTime.ToUniversalTime(),
                Extension = Path.GetExtension(fileName),
                HasDirectories = false,
                IsDirectory = false,
                Modified = info.Value.LastModified.DateTime,
                ModifiedUtc = info.Value.LastModified.UtcDateTime,
                Name = fileName,
                Path = dirName,
                Size = info.Value.ContentLength
            };
        }

        /// <summary>
        ///     Gets files and subfolders for a given path 
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public async Task<List<FileManagerEntry>> GetObjectsAsync(string path)
        {
            path = path.Trim('/');

            if (string.IsNullOrEmpty(path)) path = "/";

            var items = new List<FileManagerEntry>();

            //var dirName = Path.GetDirectoryName(path);
            // Get a reference to a directory and create it
            var directory = _shareClient.GetDirectoryClient($"/{path}");
            if (await directory.ExistsAsync())
            {
                var contents = directory.GetFilesAndDirectoriesAsync();

                await foreach (var item in contents)
                {
                    if (item.IsDirectory)
                    {
                        var subClient = directory.GetSubdirectoryClient(item.Name);

                        var subList = subClient.GetFilesAndDirectoriesAsync();

                        var hasSubdirectories = false;

                        await foreach(var i in subList)
                        {
                            if (i.IsDirectory)
                            {
                                hasSubdirectories = true;
                                break;
                            }
                        }

                        var props = await subClient.GetPropertiesAsync();

                        var fileManagerEntry = new FileManagerEntry
                        {
                            Created = props.Value.LastModified.DateTime,
                            CreatedUtc = props.Value.LastModified.UtcDateTime,
                            Extension = "",
                            HasDirectories = hasSubdirectories,
                            IsDirectory = true,
                            Modified = props.Value.LastModified.DateTime,
                            ModifiedUtc = props.Value.LastModified.UtcDateTime,
                            Name = item.Name,
                            Path = $"{path}/{item.Name}",
                            Size = 0
                        };
                        items.Add(fileManagerEntry);
                    }
                    else
                    {

                        var file = directory.GetFileClient(item.Name);
                        var info = await file.GetPropertiesAsync();

                        DateTime dateTime;
                        if (info.Value.Metadata.TryGetValue("ccmsdatetime", out string uploaded))
                        {
                            dateTime = DateTimeOffset.Parse(uploaded).UtcDateTime;
                        }
                        else
                        {
                            dateTime = info.Value.LastModified.UtcDateTime;
                        }

                        var fileManagerEntry = new FileManagerEntry
                        {
                            Created = dateTime,
                            CreatedUtc = dateTime.ToUniversalTime(),
                            Extension = Path.GetExtension(item.Name),
                            HasDirectories = false,
                            IsDirectory = false,
                            Modified = info.Value.LastModified.DateTime,
                            ModifiedUtc = info.Value.LastModified.UtcDateTime,
                            Name = item.Name,
                            Path = $"{path}/{item.Name}",
                            Size = info.Value.ContentLength
                        };
                        items.Add(fileManagerEntry);
                    }

                }
            }

            return items;
        }

        /// <inheritdoc/>
        public async Task<Stream> GetStreamAsync(string target)
        {
            if (target == "/") target = "";

            if (!string.IsNullOrEmpty(target)) target = target.TrimStart('/');

            var dirName = Path.GetDirectoryName($"/{target}");
            // Get a reference to a directory and create it
            var directory = _shareClient.GetDirectoryClient(dirName);
            if (await directory.ExistsAsync())
            {
                var fileName = Path.GetFileName(target);
                var file = directory.GetFileClient(fileName);

                if (await file.ExistsAsync())
                {
                    return await file.OpenReadAsync();
                }
                throw new Exception($"File not found: {fileName}");

            }
            throw new Exception($"Directory not found: {dirName}");
        }

        /// <inheritdoc/>
        public async Task<bool> UploadStreamAsync(Stream readStream, FileUploadMetaData fileMetaData, DateTimeOffset uploadDateTime)
        {
            // Get directory name
            var dirName = Path.GetDirectoryName(fileMetaData.RelativePath);

            // Get a reference to a directory and create it
            var directory = _shareClient.GetDirectoryClient(dirName);

            // Create if not exists
            await directory.CreateIfNotExistsAsync();

            var fileName = Path.GetFileName(fileMetaData.FileName);

            var file = directory.GetFileClient(fileName);

            var dictionaryObject = new Dictionary<string, string>();
            dictionaryObject.Add("ccmsuploaduid", fileMetaData.UploadUid);
            dictionaryObject.Add("ccmssize", fileMetaData.TotalFileSize.ToString());
            dictionaryObject.Add("ccmsdatetime", uploadDateTime.UtcDateTime.Ticks.ToString());

            await file.SetMetadataAsync(dictionaryObject);

            await file.UploadAsync(readStream);

            return true;
        }
    }
}
