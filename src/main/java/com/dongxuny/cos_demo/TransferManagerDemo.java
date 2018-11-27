package com.dongxuny.cos_demo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.CopyObjectRequest;
import com.qcloud.cos.model.CopyResult;
import com.qcloud.cos.model.GetObjectRequest;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.model.PutObjectRequest;
import com.qcloud.cos.model.UploadResult;
import com.qcloud.cos.region.Region;
import com.qcloud.cos.transfer.Copy;
import com.qcloud.cos.transfer.Download;
import com.qcloud.cos.transfer.MultipleFileDownload;
import com.qcloud.cos.transfer.MultipleFileUpload;
import com.qcloud.cos.transfer.PersistableDownload;
import com.qcloud.cos.transfer.PersistableUpload;
import com.qcloud.cos.transfer.Transfer;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos.transfer.TransferProgress;
import com.qcloud.cos.transfer.Upload;

public class TransferManagerDemo {
	public static void main(String args[]) throws FileNotFoundException {
		// uploadFile();
		// uploadDir();
		// uploadFileList();
		// pauseUploadFileAndResume();
		
		// downLoadFile();
		// downloadDir();
		// pauseDownloadFileAndResume();
		// copyFileForDiffRegion();
		// copyFileForSameRegion();
		// uploadWithServerSideEncryption();
	}
	
    // 打印进度，并且等待完成。
    private static void showTransferProgress(Transfer transfer) {
        do {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                return;
            }
            TransferProgress progress = transfer.getProgress();
            long so_far = progress.getBytesTransferred();
            long total = progress.getTotalBytesToTransfer();
            double pct = progress.getPercentTransferred();
            System.out.printf("bytes_transferred:%d, bytes_to_transfer:%d, progress:%f\n", so_far, total, pct);
        } while (transfer.isDone() == false);
        System.out.println(transfer.getState());
    }

    public static void uploadWithServerSideEncryption() throws FileNotFoundException {
        // 1 初始化用户身份信息(secretId, secretKey)
        COSCredentials cred = new BasicCOSCredentials("Your SecretId", "Your SecretKey");
        // 2 设置bucket的区域, COS地域的简称请参照 https://www.qcloud.com/document/product/436/6224
        ClientConfig clientConfig = new ClientConfig(new Region("Region where bucket located"));
        // 3 生成cos客户端
        COSClient cosclient = new COSClient(cred, clientConfig);
        // bucket名需包含appid
        String bucketName = "bucketName-appID";

        ExecutorService threadPool = Executors.newFixedThreadPool(32);
        // 传入一个threadpool, 若不传入线程池, 默认TransferManager中会生成一个单线程的线程池。
        TransferManager transferManager = new TransferManager(cosclient, threadPool);

        String key = "your object key";
        
        FileInputStream input = new FileInputStream(new File("Your File Path"));
        ObjectMetadata meta = new ObjectMetadata();
        // 目前只支持AES-256
        meta.setSSEAlgorithm("AES256");
        
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, input, meta);
        
        try {
            // 返回一个异步结果Upload, 可同步的调用waitForUploadResult等待upload结束, 成功返回UploadResult, 失败抛出异常.
            long startTime = System.currentTimeMillis();
            Upload upload = transferManager.upload(putObjectRequest);
            showTransferProgress(upload);
            UploadResult uploadResult = upload.waitForUploadResult();
            long endTime = System.currentTimeMillis();
            System.out.println("Duration: " + (endTime - startTime) / 1000);
            System.out.println("Etag: " + uploadResult.getETag());
        } catch (CosServiceException e) {
            e.printStackTrace();
        } catch (CosClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        transferManager.shutdownNow();
        cosclient.shutdown();
    }
    
    // 上传文件, 根据文件大小自动选择简单上传或者分块上传。
    public static void uploadFile() {
        // 1 初始化用户身份信息(secretId, secretKey)
        COSCredentials cred = new BasicCOSCredentials("Your SecretId", "Your SecretKey");
        // 2 设置bucket的区域, COS地域的简称请参照 https://www.qcloud.com/document/product/436/6224
        ClientConfig clientConfig = new ClientConfig(new Region("Region where bucket located"));
        // 3 生成cos客户端
        COSClient cosclient = new COSClient(cred, clientConfig);
        // bucket名需包含appid
        String bucketName = "bucketName-appID";

        ExecutorService threadPool = Executors.newFixedThreadPool(32);
        // 传入一个threadpool, 若不传入线程池, 默认TransferManager中会生成一个单线程的线程池。
        TransferManager transferManager = new TransferManager(cosclient, threadPool);

        String key = "your object key";
        File localFile = new File("File to transfer");
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, localFile);
        
        try {
            // 返回一个异步结果Upload, 可同步的调用waitForUploadResult等待upload结束, 成功返回UploadResult, 失败抛出异常.
            long startTime = System.currentTimeMillis();
            Upload upload = transferManager.upload(putObjectRequest);
            showTransferProgress(upload);
            UploadResult uploadResult = upload.waitForUploadResult();
            long endTime = System.currentTimeMillis();
            System.out.println("Duration: " + (endTime - startTime) / 1000);
            System.out.println("Etag: " + uploadResult.getETag());
        } catch (CosServiceException e) {
            e.printStackTrace();
        } catch (CosClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        transferManager.shutdownNow();
        cosclient.shutdown();
    }

    // 上传目录
    public static void uploadDir() {
        // 1 初始化用户身份信息(secretId, secretKey)
        COSCredentials cred = new BasicCOSCredentials("Your SecretId", "Your SecretKey");
        // 2 设置bucket的区域, COS地域的简称请参照 https://www.qcloud.com/document/product/436/6224
        ClientConfig clientConfig = new ClientConfig(new Region("Region where bucket located"));
        // 3 生成cos客户端
        COSClient cosclient = new COSClient(cred, clientConfig);
        // bucket名需包含appid
        String bucketName = "bucketName-appID";

        ExecutorService threadPool = Executors.newFixedThreadPool(32);
        // 传入一个threadpool, 若不传入线程池, 默认TransferManager中会生成一个单线程的线程池。
        TransferManager transferManager = new TransferManager(cosclient, threadPool);
    	
        try {
            MultipleFileUpload upload = transferManager.uploadDirectory(bucketName, "your virial directory in COS", new File("Your local directory"), true);

            showTransferProgress(upload);
        } catch (CosServiceException e) {
        	e.printStackTrace();
        } catch (CosClientException e) {
			e.printStackTrace();
		}
        transferManager.shutdownNow();
    }
    
    // 上传文件列表
    public static void uploadFileList() {
    	// 初始化文件列表
        ArrayList<File> files = new ArrayList<File>();
        String[] file_paths = {
        		"/mypath/myfile1.txt",
        		"/mypath/myfile2.txt",
        		"/mypath/myfile3.txt"};
        
        for (String path : file_paths) {
            files.add(new File(path));
        }

        // 1 初始化用户身份信息(secretId, secretKey)
        COSCredentials cred = new BasicCOSCredentials("Your SecretId", "Your SecretKey");
        // 2 设置bucket的区域, COS地域的简称请参照 https://www.qcloud.com/document/product/436/6224
        ClientConfig clientConfig = new ClientConfig(new Region("Region where bucket located"));
        // 3 生成cos客户端
        COSClient cosclient = new COSClient(cred, clientConfig);
        // bucket名需包含appid
        String bucketName = "bucketName-appID";

        ExecutorService threadPool = Executors.newFixedThreadPool(32);
        // 传入一个threadpool, 若不传入线程池, 默认TransferManager中会生成一个单线程的线程池。
        TransferManager transferManager = new TransferManager(cosclient, threadPool);
        
        try {
            MultipleFileUpload upload = transferManager.uploadFileList(bucketName, "your virial directory in COS", new File("Your local directory"), files);
            // loop with Transfer.isDone()
            showTransferProgress(upload);
        } catch (CosServiceException e) {
        	e.printStackTrace();
        } catch (CosClientException e) {
			e.printStackTrace();
		}
        transferManager.shutdownNow();
    }
    
    // 上传文件（上传过程中暂停, 并继续上传)
    public static void pauseUploadFileAndResume() {
        // 1 初始化用户身份信息(secretId, secretKey)
        COSCredentials cred = new BasicCOSCredentials("Your SecretId", "Your SecretKey");
        // 2 设置bucket的区域, COS地域的简称请参照 https://www.qcloud.com/document/product/436/6224
        ClientConfig clientConfig = new ClientConfig(new Region("Region where bucket located"));
        // 3 生成cos客户端
        COSClient cosclient = new COSClient(cred, clientConfig);
        // bucket名需包含appid
        String bucketName = "bucketName-appID";

        ExecutorService threadPool = Executors.newFixedThreadPool(4);
        // 传入一个threadpool, 若不传入线程池, 默认TransferManager中会生成一个单线程的线程池。
        TransferManager transferManager = new TransferManager(cosclient, threadPool);

        String key = "Your key";
        File localFile = new File("/mypath/myfile1.txt");
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, localFile);
        try {
            // 返回一个异步结果Upload, 可同步的调用waitForUploadResult等待upload结束, 成功返回UploadResult, 失败抛出异常.
            Upload upload = transferManager.upload(putObjectRequest);
            Thread.sleep(10000);
            PersistableUpload persistableUpload = upload.pause();
            upload = transferManager.resumeUpload(persistableUpload);
            showTransferProgress(upload);
            UploadResult uploadResult = upload.waitForUploadResult();
            System.out.println(uploadResult.getETag());
        } catch (CosServiceException e) {
            e.printStackTrace();
        } catch (CosClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        transferManager.shutdownNow();
        cosclient.shutdown();
    }

    // 将文件下载到本地
    public static void downLoadFile() {
        // 1 初始化用户身份信息(secretId, secretKey)
        COSCredentials cred = new BasicCOSCredentials("Your SecretId", "Your SecretKey");
        // 2 设置bucket的区域, COS地域的简称请参照 https://www.qcloud.com/document/product/436/6224
        ClientConfig clientConfig = new ClientConfig(new Region("Region where bucket located"));
        // 3 生成cos客户端
        COSClient cosclient = new COSClient(cred, clientConfig);
        // bucket名需包含appid
        String bucketName = "bucketName-appID";

        ExecutorService threadPool = Executors.newFixedThreadPool(32);
        // 传入一个threadpool, 若不传入线程池, 默认TransferManager中会生成一个单线程的线程池。
        TransferManager transferManager = new TransferManager(cosclient, threadPool);

        String key = "Your key";
        File downloadFile = new File("Your local file path");
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
        try {
            // 返回一个异步结果copy, 可同步的调用waitForCompletion等待download结束, 成功返回void, 失败抛出异常.
            Download download = transferManager.download(getObjectRequest, downloadFile);
            download.waitForCompletion();
        } catch (CosServiceException e) {
            e.printStackTrace();
        } catch (CosClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        transferManager.shutdownNow();
        cosclient.shutdown();
    }

    // 将文件目录下载到本地
    public static void downloadDir() {
        // 1 初始化用户身份信息(secretId, secretKey)
        COSCredentials cred = new BasicCOSCredentials("Your SecretId", "Your SecretKey");
        // 2 设置bucket的区域, COS地域的简称请参照 https://www.qcloud.com/document/product/436/6224
        ClientConfig clientConfig = new ClientConfig(new Region("Region where bucket located"));
        // 3 生成cos客户端
        COSClient cosclient = new COSClient(cred, clientConfig);
        // bucket名需包含appid
        String bucketName = "bucketName-appID";

        ExecutorService threadPool = Executors.newFixedThreadPool(32);
        // 传入一个threadpool, 若不传入线程池, 默认TransferManager中会生成一个单线程的线程池。
        TransferManager transferManager = new TransferManager(cosclient, threadPool);
    	
        try {
            MultipleFileDownload download = transferManager.downloadDirectory(bucketName, "your virial directory in COS", new File("Your local directory"));
            // loop with Transfer.isDone()
            showTransferProgress(download);
            download.waitForCompletion();
        } catch (CosServiceException e) {
        	e.printStackTrace();
        } catch (CosClientException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        transferManager.shutdownNow();
    }
    
    // 将文件下载到本地(中途暂停并继续开始)
    public static void pauseDownloadFileAndResume() {
        // 1 初始化用户身份信息(secretId, secretKey)
        COSCredentials cred = new BasicCOSCredentials("Your SecretId", "Your SecretKey");
        // 2 设置bucket的区域, COS地域的简称请参照 https://www.qcloud.com/document/product/436/6224
        ClientConfig clientConfig = new ClientConfig(new Region("Region where bucket located"));
        // 3 生成cos客户端
        COSClient cosclient = new COSClient(cred, clientConfig);
        // bucket名需包含appid
        String bucketName = "bucketName-appID";

        ExecutorService threadPool = Executors.newFixedThreadPool(32);
        // 传入一个threadpool, 若不传入线程池, 默认TransferManager中会生成一个单线程的线程池。
        TransferManager transferManager = new TransferManager(cosclient, threadPool);

        String key = "Your key";
        File downloadFile = new File("Your local file path");
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
        try {
            // 返回一个异步结果copy, 可同步的调用waitForCompletion等待download结束, 成功返回void, 失败抛出异常.
            Download download = transferManager.download(getObjectRequest, downloadFile);
            Thread.sleep(5000L);
            PersistableDownload persistableDownload = download.pause();
            download = transferManager.resumeDownload(persistableDownload);
            showTransferProgress(download);
            download.waitForCompletion();
        } catch (CosServiceException e) {
            e.printStackTrace();
        } catch (CosClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        transferManager.shutdownNow();
        cosclient.shutdown();
    }
    
    // copy接口支持根据文件大小自动选择copy或者分块copy
    // 以下代码展示跨园区拷贝, 即将一个园区的文件拷贝到另一个园区
    public static void copyFileForDiffRegion() {
        // 1 初始化用户身份信息(secretId, secretKey)
        COSCredentials cred = new BasicCOSCredentials("Your SecretId", "Your SecretKey");
        // 2 设置bucket的区域, COS地域的简称请参照 https://www.qcloud.com/document/product/436/6224
        ClientConfig clientConfig = new ClientConfig(new Region("Region where bucket located"));
        // 3 生成cos客户端
        COSClient cosclient = new COSClient(cred, clientConfig);


        ExecutorService threadPool = Executors.newFixedThreadPool(32);
        // 传入一个threadpool, 若不传入线程池, 默认TransferManager中会生成一个单线程的线程池。
        TransferManager transferManager = new TransferManager(cosclient, threadPool);

        // 要拷贝的bucket region, 支持跨园区拷贝
        Region srcBucketRegion = new Region("Region where bucket located");
        // 源bucket, bucket名需包含appid
        String srcBucketName = "srcBucket-appId";
        // 要拷贝的源文件
        String srcKey = "Your source key";
        // 目的bucket, bucket名需包含appid
        String destBucketName = "destBucket-appId";
        // 要拷贝的目的文件
        String destKey = "Your destination key";

        COSClient srcCOSClient = new COSClient(cred, new ClientConfig(srcBucketRegion));
        CopyObjectRequest copyObjectRequest = new CopyObjectRequest(srcBucketRegion, srcBucketName,
                srcKey, destBucketName, destKey);
        try {
            Copy copy = transferManager.copy(copyObjectRequest, srcCOSClient, null);
            // 返回一个异步结果copy, 可同步的调用waitForCopyResult等待copy结束, 成功返回CopyResult, 失败抛出异常.
            CopyResult copyResult = copy.waitForCopyResult();
        } catch (CosServiceException e) {
            e.printStackTrace();
        } catch (CosClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        transferManager.shutdownNow();
        srcCOSClient.shutdown();
        cosclient.shutdown();
    }

    // copy接口支持根据文件大小自动选择copy或者分块copy
    // 以下代码展示同园区拷贝, 即将同园区的文件拷贝到另一个园区
    public static void copyFileForSameRegion() {
        // 1 初始化用户身份信息(secretId, secretKey)
        COSCredentials cred = new BasicCOSCredentials("Your SecretId", "Your SecretKey");
        // 2 设置bucket的区域, COS地域的简称请参照 https://www.qcloud.com/document/product/436/6224
        ClientConfig clientConfig = new ClientConfig(new Region("Region where bucket located"));
        // 3 生成cos客户端
        COSClient cosclient = new COSClient(cred, clientConfig);


        ExecutorService threadPool = Executors.newFixedThreadPool(32);
        // 传入一个threadpool, 若不传入线程池, 默认TransferManager中会生成一个单线程的线程池。
        TransferManager transferManager = new TransferManager(cosclient, threadPool);

        // 要拷贝的bucket region, 支持跨园区拷贝
        Region srcBucketRegion = new Region("Region where bucket located");
        // 源bucket, bucket名需包含appid
        String srcBucketName = "srcBucket-appId";
        // 要拷贝的源文件
        String srcKey = "Your source key";
        // 目的bucket, bucket名需包含appid
        String destBucketName = "destBucket-appId";
        // 要拷贝的目的文件
        String destKey = "Your destination key";

        CopyObjectRequest copyObjectRequest = new CopyObjectRequest(srcBucketRegion, srcBucketName,
                srcKey, destBucketName, destKey);
        try {
            Copy copy = transferManager.copy(copyObjectRequest);
            // 返回一个异步结果copy, 可同步的调用waitForCopyResult等待copy结束, 成功返回CopyResult, 失败抛出异常.
            CopyResult copyResult = copy.waitForCopyResult();
        } catch (CosServiceException e) {
            e.printStackTrace();
        } catch (CosClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        transferManager.shutdownNow();
        cosclient.shutdown();
    }
}
