package neu.mr.cs6240.aws;

import static neu.mr.cs6240.Constants.NetworkCC.S3_START_PATH;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;

/**
 *
 * @author ajay subramanya
 *
 */
public class S3 {
	private final static Logger logger = Logger.getLogger(S3.class);
	private static final int BUFFER_SIZE = 128 * 1024 * 1024; // 128MB

	private AmazonS3Client s3;

	public S3() {
		s3 = new AmazonS3Client();
	}

	/**
	 * @author steffen opel
	 * @author ajay subramanya
	 *
	 * @param bucketName
	 *            the name of the user bucket
	 * @param path
	 *            the key to look for in the bucket
	 * @return if the key exists in the bucket or not
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 */
	public boolean isValidFile(String bucketName, String path) throws AmazonClientException, AmazonServiceException {
		try {
			s3.getObjectMetadata(bucketName, path);
		} catch (AmazonS3Exception s3e) {
			if (s3e.getStatusCode() == 404)
				return false;
			else
				throw s3e;
		}
		return true;
	}

	/**
	 * Prints all objects in given bucketName and prefix path
	 *
	 * @param bucketName
	 *            the name of the bucket
	 * @param prefix
	 *            the path of the files
	 * @return list of files in bucket
	 */
	public List<String> listObjsInBucket(String bucketName, String prefix)
	        throws AmazonServiceException, AmazonClientException {
		List<String> files = new ArrayList<>();
		ObjectListing objectListing = null;
		if (StringUtils.isNotEmpty(prefix)) {
			objectListing = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(prefix));
		} else {
			objectListing = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName));
		}

		for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
			files.add(objectSummary.getKey());
		}
		return files;
	}

	/**
	 * Prints all objects in given bucketName and prefix path
	 *
	 * @param bucketName
	 * @param prefix
	 */
	public List<FileNameSizeObj> getS3FileObjects(String bucketName, String prefix) {
		List<FileNameSizeObj> files = new ArrayList<>();
		try {
			ObjectListing objectListing = s3
			        .listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(prefix));
			for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
				long sz = objectSummary.getSize();
				System.out.println(objectSummary.getKey() + "  " + "(size = " + sz + ")");
				String key = objectSummary.getKey();

				if (objectSummary.getSize() != 0 && key.contains(prefix + "/")) {
					String[] part = key.split("/");
					int len = part.length;
					String folder = "";
					for (int i = 0; i < len - 1; i++) {
						folder += part[i] + "/";
					}
					String fileName = part[len - 1];
					if (folder.equals(prefix + "/")) files.add(new FileNameSizeObj(fileName, sz));
				}
			}

		} catch (AmazonServiceException ase) {
			printServiceException(ase);
		} catch (AmazonClientException ace) {
			printClientException(ace);
		}
		Collections.sort(files, Collections.reverseOrder());
		return files;
	}

	/**
	 * Delete a file from bucket name
	 *
	 * @param bucketName
	 *            - s3 Bucket name
	 * @param key
	 *            - fileName
	 */
	public void deleteFile(String bucketName, String key) {
		try {
			s3.deleteObject(bucketName, key);
		} catch (AmazonServiceException ase) {
			printServiceException(ase);
		} catch (AmazonClientException ace) {
			printClientException(ace);
		}
	}

	/**
	 * @author https://javatutorial.net/java-s3-example
	 * @param bucketName
	 * @param folderName
	 */
	public void createFolder(String bucketName, String folderName) {
		try {
			// create meta-data for your folder and set content-length to 0
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(0);
			// create empty content
			InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
			// create a PutObjectRequest passing the folder name suffixed by /
			PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, folderName + "/", emptyContent,
			        metadata);
			// send request to S3Operations to create folder
			s3.putObject(putObjectRequest);
		} catch (AmazonServiceException ase) {
			printServiceException(ase);
		} catch (AmazonClientException ace) {
			printClientException(ace);
		}
	}

	/**
	 * Downloads a directory from s3 to local destination path
	 *
	 * @author ajay subramanya
	 *
	 * @param bucketName
	 * @param dirNameOnS3
	 * @param destlocalDirPath
	 */
	public void downloadDirectory(String bucketName, String dirNameOnS3, File destlocalDirPath) {
		TransferManager tx = new TransferManager();
		MultipleFileDownload download = tx.downloadDirectory(bucketName, dirNameOnS3, destlocalDirPath);
		try {
			logger.info("downloading " + bucketName + "/" + dirNameOnS3);
			download.waitForCompletion();
		} catch (Exception e) {
			logger.error("error while downlaoding file " + bucketName + "/" + dirNameOnS3 + " " + e.getMessage());
		}
		logger.info("downloaded");
		tx.shutdownNow();
	}

	/**
	 * Uploads a directory from local to s3
	 *
	 * @author ajay subramanya
	 * @param bucketName
	 * @param dirNameOnS3
	 * @param inputFilePath
	 *
	 *            Or you can block the current thread and wait for your transfer
	 *            to complete. If the transfer fails, this method will throw an
	 *            AmazonClientException or AmazonServiceException detailing the
	 *            reason.
	 */
	public void uploadDirectory(String bucketName, String dirNameOnS3, File inputFilePath) {
		TransferManager tx = null;
		try {
			tx = new TransferManager();
			MultipleFileUpload myUpload = tx.uploadDirectory(bucketName, dirNameOnS3, inputFilePath, true);
			myUpload.waitForCompletion();
		} catch (InterruptedException e) {
			logger.error("Exception while uploading directory " + inputFilePath + " \n " + e.getMessage());
		} finally {
			if (tx != null) {
				tx.shutdownNow();
			}
		}
	}

	/**
	 * @author ajay subramanya
	 * @param path
	 *            the path of the s3 bucket
	 * @return the bucket name and the key as a list of string
	 */
	public List<String> splitS3Path(String path) {
		List<String> str = new ArrayList<>();
		String bucketName = StringUtils.substringBetween(path, "s3://", "/");
		String key = StringUtils.substringAfter(path, bucketName + "/");
		if (StringUtils.isNotEmpty(bucketName)) str.add(bucketName);
		if (StringUtils.isNotEmpty(key)) str.add(key);
		return str;
	}

	/**
	 * Parses s3 prefix name after bucket
	 *
	 * @param inputPath
	 * @return
	 */
	public String getPrefixName(String inputPath, String bucketName) {
		return StringUtils.substring(inputPath, (S3_START_PATH + bucketName + "/").length());
	}

	/**
	 * Parses s3 bucket name after s3://
	 *
	 * @param inputPath
	 * @return
	 */
	public String getBucketName(String inputPath) {
		return StringUtils.substringBetween(inputPath, S3_START_PATH, "/");
	}

	/**
	 * Copying the S3 File locally
	 *
	 * @param bucketName
	 * @param key
	 * @param outputFileName
	 * @throws IOException
	 */
	public void readS3CopyLocal(String bucketName, String key, String outputFileName) {

		try {
			S3Object s3object = s3.getObject(new GetObjectRequest(bucketName, key));

			InputStream stream = s3object.getObjectContent();
			byte[] content = new byte[BUFFER_SIZE];

			BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(outputFileName));
			int totalSize = 0;
			int bytesRead;
			while ((bytesRead = stream.read(content)) != -1) {
				outputStream.write(content, 0, bytesRead);
				totalSize += bytesRead;
			}
			logger.info("Total Size of file in bytes = " + totalSize);
			outputStream.close();
		} catch (AmazonServiceException ase) {
			printServiceException(ase);
		} catch (AmazonClientException ace) {
			printClientException(ace);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Deletes multiple files at one shot. <br>
	 * NOTE : There is no folder concept in s3 everything is files. So List
	 * <String> filesToDelete takes the files in given bucket to delete.<br>
	 * If a file version is specified then only that version of file can be
	 * deleted. <br>
	 * Ex: To delete a folder(Ex: MapInput i.e.concept we have in mind like
	 * s3://mr6240/MapInput) we first need to get all the string list file paths
	 * in that folder using
	 * ListObjectsRequest().withBucketName(bucketName).withPrefix(folderName)
	 * and then call this function.
	 *
	 * @param bucketName
	 * @param filesToDelete
	 *
	 * @author smitha
	 */
	public void deleteMultipleFiles(String bucketName, List<String> filesToDelete) {
		if (filesToDelete == null) {
			if (logger.isDebugEnabled()) {
				logger.debug("No files to delete. Null");
			}
			return;
		}

		DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(bucketName);

		List<KeyVersion> keys = new ArrayList<KeyVersion>();
		for (String keyName : filesToDelete)
			keys.add(new KeyVersion(keyName));

		multiObjectDeleteRequest.setKeys(keys);

		try {
			DeleteObjectsResult delObjRes = s3.deleteObjects(multiObjectDeleteRequest);
			logger.info("Successfully deleted " + delObjRes.getDeletedObjects().size() + " items from s3");
		} catch (MultiObjectDeleteException e) {
			printServiceException(e);
		}
	}

	/**
	 * Uploads a file to s3 bucket
	 * 
	 * @param bucketName
	 * @param key
	 * @param aFile
	 */
	public void uploadFile(String bucketName, String key, File aFile) {
		try {
			s3.putObject(new PutObjectRequest(bucketName, key, aFile));
		} catch (AmazonServiceException ase) {
			printServiceException(ase);
		} catch (AmazonClientException ace) {
			printClientException(ace);
		}
	}

	/**
	 * Prints AmazonClientException details
	 *
	 * @param ace
	 */
	private void printClientException(AmazonClientException ace) {
		logger.error("Caught an AmazonClientException, which means the client encountered "
		        + "a serious internal problem while trying to communicate with , "
		        + "such as not being able to access the network.");
		logger.error("Error Message: " + ace.getMessage());
	}

	/**
	 * Prints AmazonServiceException details
	 *
	 * @param ase
	 */
	private void printServiceException(AmazonServiceException ase) {
		logger.error("Caught an AmazonServiceException, which means your request made it "
		        + "to Amazon , but was rejected with an error response for some reason.");
		logger.error("Error Message:    " + ase.getMessage());
		logger.error("HTTP Status Code: " + ase.getStatusCode());
		logger.error("AWS Error Code:   " + ase.getErrorCode());
		logger.error("Error Type:       " + ase.getErrorType());
		logger.error("Request ID:       " + ase.getRequestId());
	}

}
