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

import software.amazon.awssdk.services.s3.model.OutputSerialization;
import software.amazon.awssdk.services.s3.model.RecordsEvent;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.SelectObjectContentEventStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponse;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponseHandler;

@Service
@RequiredArgsConstructor
@Slf4j
public class S3Services {
	
	private final  S3Client s3Client;
	private final S3AsyncClient s3AsyncClient;
	private @Value("${aws.s3.bucket}") String bucket;
	private @Value("${aws.s3.prefix}") String prefix;	
	
	
	@SneakyThrows
	public void getListFilesFromDirectory() {
		log.info("prefix to search: {}",this.prefix);
		ListObjectsRequest request= ListObjectsRequest.builder()
				.bucket(this.bucket)
				.prefix(this.prefix).maxKeys(3)				
				.build();		
		ListObjectsResponse response= s3Client.listObjects(request);		
		List<S3Object> listObject=response.contents();
		for (S3Object s3Object : listObject) {
			
			log.info(s3Object.key());			
			InputSerialization inputSerialization = InputSerialization.builder()
					.json(JSONInput.builder().type("DOCUMENT").build())
					.build();
			
			OutputSerialization outputSerialization=  OutputSerialization.builder()
					.json(JSONOutput.builder().build())
					.build();
			
			SelectObjectContentRequest requestS3Select= SelectObjectContentRequest.builder()
					.bucket("nu0286001-central-channel-repository-qa-daily-raw-data")
					.key(s3Object.key())
					.expression("SELECT * FROM s3object s WHERE (s.deviceNameId='APP' AND s.transactionCode='0605') OR (s.deviceNameId='APP' AND s.transactionCode='0705') LIMIT 5")
					.expressionType(ExpressionType.SQL)
					.inputSerialization(inputSerialization)
					.outputSerialization(outputSerialization)
					.build();
			
			final TestHandler handler = new TestHandler();		
			
			s3AsyncClient.selectObjectContent(requestS3Select,handler).get();
			RecordsEvent responseEvent = (RecordsEvent) handler.receivedEvents.stream()
		            .filter(e -> e.sdkEventType() == SelectObjectContentEventStream.EventType.RECORDS)		            
		            .findFirst()
		            .orElse(null);
			if(responseEvent!=null) {
				log.info(responseEvent.payload().asUtf8String());
				break;
			}			
		}
		
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

/*
response.contents().forEach(o->{
	log.info(o.key());			
	InputSerialization inputSerialization = InputSerialization.builder()
			.json(JSONInput.builder().type("DOCUMENT").build())
			.build();
	
	OutputSerialization outputSerialization=  OutputSerialization.builder()
			.json(JSONOutput.builder().build())
			.build();
	
	SelectObjectContentRequest requestS3Select= SelectObjectContentRequest.builder()
			.bucket("nu0286001-central-channel-repository-qa-daily-raw-data")
			.key(o.key())
			.expression("SELECT * FROM s3object s WHERE (s.deviceNameId='APP' AND s.transactionCode='0605') OR (s.deviceNameId='APP' AND s.transactionCode='0705') LIMIT 5")
			.expressionType(ExpressionType.SQL)
			.inputSerialization(inputSerialization)
			.outputSerialization(outputSerialization)
			.build();
	
	final TestHandler handler = new TestHandler();		
	try {
		s3AsyncClient.selectObjectContent(requestS3Select,handler).get();
		RecordsEvent responseEvent = (RecordsEvent) handler.receivedEvents.stream()
	            .filter(e -> e.sdkEventType() == SelectObjectContentEventStream.EventType.RECORDS)			            
	            .findFirst()
	            .orElse(null);
		if(responseEvent!=null) {
			log.info(responseEvent.payload().asUtf8String());					
		}
	} catch (InterruptedException | ExecutionException e) {				
		log.error(e.getMessage(),e);
	}
	
});
*/

