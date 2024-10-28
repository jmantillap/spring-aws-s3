package work.javiermantilla.aws.s3;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ExpressionType;
import software.amazon.awssdk.services.s3.model.InputSerialization;
import software.amazon.awssdk.services.s3.model.JSONInput;
import software.amazon.awssdk.services.s3.model.JSONOutput;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.OutputSerialization;
import software.amazon.awssdk.services.s3.model.RecordsEvent;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.SelectObjectContentEventStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponse;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponseHandler;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

@Service
@RequiredArgsConstructor
@Slf4j
public class S3Services {

	private final S3Client s3Client;
	private final S3AsyncClient s3AsyncClient;
	private @Value("${aws.s3.bucket}") String bucket;	
	private String prefix ="dev/test/message";
	private String expression="SELECT * FROM s3object s WHERE s.sessionId='5ab7356d-5a5f-4c27-bc37-f93073770978' LIMIT 5";
	
	private static final String TYPE_DOCUMENT="DOCUMENT";

	@SneakyThrows
	public void getListV2PaginateFilesFromDirectory() {
		//https://www.baeldung.com/java-aws-s3-list-bucket-objects
		this.printPrefix();
		InputSerialization inputSerialization = InputSerialization.builder()
				.json(JSONInput.builder().type(TYPE_DOCUMENT).build()).build();

		OutputSerialization outputSerialization = OutputSerialization.builder().json(JSONOutput.builder().build())
				.build();
		ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
				.bucket(bucket)
				.prefix(prefix)
				.maxKeys(10) // Set the maxKeys parameter to control the page size
				.build();
		boolean isMessage = false;
		ListObjectsV2Iterable listObjectsV2Iterable = s3Client.listObjectsV2Paginator(listObjectsV2Request);
		for (ListObjectsV2Response page : listObjectsV2Iterable) {
			List<S3Object> listObject = page.contents();
			for (S3Object s3Object : listObject) {
				log.info(s3Object.key());
				SelectObjectContentRequest.Builder requestS3Select = SelectObjectContentRequest.builder()
						.bucket(this.bucket)
						.key(s3Object.key())
						.expression(this.expression)
						.expressionType(ExpressionType.SQL)
						.inputSerialization(inputSerialization)
						.outputSerialization(outputSerialization);
						
				final TestHandler handler = new TestHandler();
				s3AsyncClient.selectObjectContent(requestS3Select.build(), handler).get();
				RecordsEvent responseEvent = (RecordsEvent) handler.receivedEvents.stream()
						.filter(e -> e.sdkEventType() == SelectObjectContentEventStream.EventType.RECORDS).findFirst()
						.orElse(null);
				if (responseEvent != null) {
					log.info(responseEvent.payload().asUtf8String());
					break;
				}
			}
			if(isMessage) {
				break;
			}

		}
	}
	
	@SneakyThrows
	public void getListV2FilesFromDirectory() {
		//https://www.baeldung.com/java-aws-s3-list-bucket-objects
		// long totalObjects = 0;
		this.printPrefix();
		String nextContinuationToken = null;
		
		boolean isMessage = false;
		InputSerialization inputSerialization = InputSerialization.builder()
				.json(JSONInput.builder().type(TYPE_DOCUMENT).build()).build();

		OutputSerialization outputSerialization = OutputSerialization.builder().json(JSONOutput.builder().build())
				.build();
		do {
			ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
					.bucket(bucket)
					.prefix(prefix)
					.maxKeys(5)
					.continuationToken(nextContinuationToken);

			ListObjectsV2Response response = s3Client.listObjectsV2(requestBuilder.build());
			nextContinuationToken = response.nextContinuationToken();
			List<S3Object> listObject = response.contents();
			// totalObjects+=listObject.size();

			for (S3Object s3Object : listObject) {
				log.info(s3Object.key());
				SelectObjectContentRequest requestS3Select = SelectObjectContentRequest.builder()
						.bucket(this.bucket)
						.key(s3Object.key())
						.expression(this.expression).expressionType(ExpressionType.SQL)
						.inputSerialization(inputSerialization)
						.outputSerialization(outputSerialization)
						.build();
				final TestHandler handler = new TestHandler();

				s3AsyncClient.selectObjectContent(requestS3Select, handler).get();
				RecordsEvent responseEvent = (RecordsEvent) handler.receivedEvents.stream()
						.filter(e -> e.sdkEventType() == SelectObjectContentEventStream.EventType.RECORDS)
						.findFirst()
						.orElse(null);
				if (responseEvent != null) {
					log.info(responseEvent.payload().asUtf8String());
					isMessage = true;
					break;
				}
			}
			if (isMessage)
				break;

		} while (nextContinuationToken != null);
		// log.info("Total de registros en el bucket: {}",totalObjects);

	}

	@SneakyThrows
	public void getListFilesFromDirectory() {
		this.printPrefix();
		ListObjectsRequest request = ListObjectsRequest.builder()
				.bucket(this.bucket)
				.prefix(this.prefix)
				.maxKeys(10)
				.build();
		ListObjectsResponse response = s3Client.listObjects(request);
		List<S3Object> listObject = response.contents();
		InputSerialization inputSerialization = InputSerialization.builder()
				.json(JSONInput.builder().type(TYPE_DOCUMENT).build()).build();

		OutputSerialization outputSerialization = OutputSerialization.builder().json(JSONOutput.builder().build())
				.build();
		for (S3Object s3Object : listObject) {
			log.info(s3Object.key());
			SelectObjectContentRequest requestS3Select = SelectObjectContentRequest.builder().bucket(this.bucket)
					.key(s3Object.key())
					.expression(this.expression)
					.expressionType(ExpressionType.SQL)
					.inputSerialization(inputSerialization)
					.outputSerialization(outputSerialization)
					.build();
			final TestHandler handler = new TestHandler();

			s3AsyncClient.selectObjectContent(requestS3Select, handler).get();
			RecordsEvent responseEvent = (RecordsEvent) handler.receivedEvents.stream()
					.filter(e -> e.sdkEventType() == SelectObjectContentEventStream.EventType.RECORDS)
					.findFirst()
					.orElse(null);
			if (responseEvent != null) {
				log.info(responseEvent.payload().asUtf8String());
				break;
			}
		}

	}
	
	private void printPrefix() {
		log.info("prefix to search: {}", this.prefix);
	}

	private static class TestHandler implements SelectObjectContentResponseHandler {
		@SuppressWarnings("unused")
		private SelectObjectContentResponse response;
		private List<SelectObjectContentEventStream> receivedEvents = new ArrayList<>();
		@SuppressWarnings("unused")
		private Throwable exception;

		@Override
		public void responseReceived(SelectObjectContentResponse response) {
			this.response = response;
		}

		@Override
		public void onEventStream(SdkPublisher<SelectObjectContentEventStream> publisher) {
			publisher.subscribe(receivedEvents::add);
		}

		@Override
		public void exceptionOccurred(Throwable throwable) {
			exception = throwable;
		}

		@Override
		public void complete() {
		}
	}

}


