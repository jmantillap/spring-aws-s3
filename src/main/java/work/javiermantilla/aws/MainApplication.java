package work.javiermantilla.aws;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import work.javiermantilla.aws.s3.S3Services;

@Log4j2
@Component
@RequiredArgsConstructor
public class MainApplication implements CommandLineRunner {

	private final S3Services s3Services;
	
	@Override
	public void run(String... args) throws Exception {
		
		s3Services.getListV2FilesFromDirectory();
	}

}
