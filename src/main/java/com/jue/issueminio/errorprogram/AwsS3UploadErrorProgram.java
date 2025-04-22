package com.jue.issueminio.errorprogram;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.utils.IoUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.*;

/**
 * @author JUE
 * @date 2025/4/22
 */
public class AwsS3UploadErrorProgram {

    // minio version RELEASE.2025-04-08T15-41-24Z
    public static final String URL = "url";
    public static final String REGION = "my-region";
    public static final String ACCESS_KEY_ID = "******";
    public static final String SECRET_ACCESS_KEY = "******";

    public static final String RESOURCES_FILE_PATH = "errorprogram/fail.txt";
    public static final String RESOURCES_FILE_PATH_SUCCESS = "errorprogram/success.txt";

    /**
     * 测试文件上传异常
     *
     * @param args test
     */
    public static void main(String[] args) throws IOException {
        String bucketName = "bucket-test";
        S3Client awsS3Client = before(bucketName);
        // success
        multipartUpload(awsS3Client, bucketName, RESOURCES_FILE_PATH_SUCCESS, RESOURCES_FILE_PATH_SUCCESS);
        System.out.println("first is successful");
        // fail
        multipartUpload(awsS3Client, bucketName, RESOURCES_FILE_PATH, RESOURCES_FILE_PATH);
        System.out.println("second is successful");
    }

    /**
     * 创建连接及创建桶
     *
     * @param bucketName 桶
     */
    public static S3Client before(String bucketName) {
        S3Client awsS3Client = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY_ID, SECRET_ACCESS_KEY)))
                .region(Region.of(REGION))
                .endpointOverride(URI.create(URL))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(false).build())
                .build();
        // 如存储桶不存在，创建之。
        ListBucketsResponse response = awsS3Client.listBuckets();
        Set<String> exists = new HashSet<>();
        if (response.hasBuckets()) {
            for (Bucket bucket : response.buckets()) {
                exists.add(bucket.name());
            }
        }
        if (!exists.contains(bucketName)) {
            awsS3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        }
        return awsS3Client;
    }

    /**
     * java端分片上传
     */
    public static void multipartUpload(S3Client awsS3Client, String bucketName, String objectKey, String resourcesFilePath) throws IOException {
        int partSize = 50 * 1024 * 1024;

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (InputStream in = AwsS3UploadErrorProgram.class.getResourceAsStream("/" + resourcesFilePath)) {
            if (in == null) {
                System.out.println("error!");
                return;
            }
            IoUtils.copy(in, outputStream);
        }
        byte[] content = outputStream.toByteArray();
        long totalSize = content.length;

        long subSize = totalSize % partSize;
        long partNum = totalSize / partSize + (subSize > 0 ? 1 : 0);

        // 分片上传测试用例
        CreateMultipartUploadResponse createMultipartUploadResponse = awsS3Client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build());
        String uploadId = createMultipartUploadResponse.uploadId();
        System.out.println("uploadId ===> " + uploadId);
        List<CompletedPart> completedPartList = new ArrayList<>();
        for (int index = 0; index < partNum; index++) {
            long currentSize = index == partNum - 1 && subSize > 0 ? subSize : partSize;
            byte[] currentByte = Arrays.copyOfRange(content, partSize * index, (int) (partSize * index + currentSize));

            UploadPartResponse uploadPartResponse = awsS3Client.uploadPart(UploadPartRequest.builder()
                    .uploadId(uploadId)
                    .bucket(bucketName)
                    .key(objectKey)
                    .partNumber(index + 1)
                    .contentLength(currentSize)
                    .build(), RequestBody.fromBytes(currentByte));
            completedPartList.add(CompletedPart.builder()
                    .partNumber(index + 1)
                    .eTag(uploadPartResponse.eTag())
                    .build());
        }

        awsS3Client.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                .uploadId(uploadId)
                .bucket(bucketName)
                .key(objectKey)
                .multipartUpload(CompletedMultipartUpload.builder().parts(completedPartList).build())
                .build());
    }
}
