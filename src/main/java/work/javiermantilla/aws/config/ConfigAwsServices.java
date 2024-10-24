package work.javiermantilla.aws.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.log4j.Log4j2;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

@Configuration
@Log4j2
public class ConfigAwsServices {
	
	private @Value("${aws.accessKeyId}") String accessKey;
	private @Value("${aws.secretKey}") String secretKey;
	private @Value("${aws.tokenSession}") String tokenSession;

	@Bean
	S3Client getS3Client() {
		
		return S3Client.builder()
				.credentialsProvider(StaticCredentialsProvider.create(this.getCredentials()))
				.region(Region.US_EAST_1).build();
	}
	
	@Bean
	S3AsyncClient getS3AsyncClient() {
		return S3AsyncClient.builder()
				.credentialsProvider(StaticCredentialsProvider.create(this.getCredentials()))
				.region(Region.US_EAST_1).build();
	}
	
	private AwsSessionCredentials getCredentials() {
		log.info(this.accessKey);
		return AwsSessionCredentials.builder()
				.accessKeyId(this.accessKey)
				.secretAccessKey(this.secretKey)
				.sessionToken(this.tokenSession)
				.build();
	}

}
