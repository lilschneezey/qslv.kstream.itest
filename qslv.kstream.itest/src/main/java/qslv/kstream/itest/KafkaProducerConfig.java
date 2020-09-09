package qslv.kstream.itest;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import com.fasterxml.jackson.databind.JavaType;

import qslv.common.kafka.JacksonAvroSerializer;
import qslv.common.kafka.TraceableMessage;
import qslv.data.Account;
import qslv.data.BalanceLog;
import qslv.data.OverdraftInstruction;
import qslv.kstream.LoggedTransaction;
import qslv.kstream.PostingRequest;

@Configuration
public class KafkaProducerConfig {
	private static final Logger log = LoggerFactory.getLogger(KafkaProducerConfig.class);

	@Autowired
	ConfigProperties configProperties;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Bean
	public Map<String,Object> producerConfig() throws Exception {
		Properties kafkaconfig = new Properties();
		try {
			kafkaconfig.load(new FileInputStream(configProperties.getKafkaProducerPropertiesPath()));
		} catch (Exception fileEx) {
			try {
				kafkaconfig.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(configProperties.getKafkaProducerPropertiesPath()));
			} catch (Exception resourceEx) {
				log.error("{} not found.", configProperties.getKafkaProducerPropertiesPath());
				log.error("File Exception. {}", fileEx.toString());
				log.error("Resource Exception. {}", resourceEx.toString());
				throw resourceEx;
			}
		}
		return new HashMap(kafkaconfig);
	}
	
	// Request
	@Bean
	public ProducerFactory<String, TraceableMessage<PostingRequest>> requestProducerFactory() throws Exception {
		JacksonAvroSerializer<TraceableMessage<PostingRequest>> jas = new JacksonAvroSerializer<>();
		JavaType type = jas.getTypeFactory().constructParametricType(TraceableMessage.class, PostingRequest.class);
		jas.configure(producerConfig(), false, type);
		return new DefaultKafkaProducerFactory<String, TraceableMessage<PostingRequest>>(producerConfig(),
				new StringSerializer(), jas);
	}

	@Bean
	public KafkaTemplate<String, TraceableMessage<PostingRequest>> requestKafkaTemplate(
			ProducerFactory<String, TraceableMessage<PostingRequest>> requestProducerFactory) throws Exception {
		return new KafkaTemplate<>(requestProducerFactory, true); // auto-flush true, to force each message to broker.
	}	
	
	// Account
	@Bean
	public ProducerFactory<String, Account> accountProducerFactory() throws Exception {
		JacksonAvroSerializer<Account> jas = new JacksonAvroSerializer<>();
		jas.configure(producerConfig(), false);
		return new DefaultKafkaProducerFactory<String, Account>(producerConfig(),
				new StringSerializer(), jas);
	}

	@Bean
	public KafkaTemplate<String, Account> accountKafkaTemplate(ProducerFactory<String, Account> accountProducerFactory) throws Exception {
		return new KafkaTemplate<>(accountProducerFactory, true); // auto-flush true, to force each message to broker.
	}	
	
	// Overdraft
	@Bean
	public ProducerFactory<String, OverdraftInstruction> overdraftProducerFactory() throws Exception {
		JacksonAvroSerializer<OverdraftInstruction> jas = new JacksonAvroSerializer<>();
		jas.configure(producerConfig(), false);
		return new DefaultKafkaProducerFactory<String, OverdraftInstruction>(producerConfig(),
				new StringSerializer(), jas);
	}

	@Bean
	public KafkaTemplate<String, OverdraftInstruction> overdraftKafkaTemplate(ProducerFactory<String, OverdraftInstruction> overdraftProducerFactory) throws Exception {
		return new KafkaTemplate<>(overdraftProducerFactory, true); // auto-flush true, to force each message to broker.
	}	
	
	// LoggedTransaction
	@Bean
	public ProducerFactory<String, LoggedTransaction> transactionProducerFactory() throws Exception {
		JacksonAvroSerializer<LoggedTransaction> jas = new JacksonAvroSerializer<>();
		jas.configure(producerConfig(), false);
		return new DefaultKafkaProducerFactory<String, LoggedTransaction>(producerConfig(),
				new StringSerializer(), jas);
	}

	@Bean
	public KafkaTemplate<String, LoggedTransaction> transactionKafkaTemplate(ProducerFactory<String, LoggedTransaction> transactionProducerFactory) throws Exception {
		return new KafkaTemplate<>(transactionProducerFactory, true); // auto-flush true, to force each message to broker.
	}	
	
	// Balance Log
	@Bean
	public ProducerFactory<String, BalanceLog> balanceLogProducerFactory() throws Exception {
		JacksonAvroSerializer<BalanceLog> jas = new JacksonAvroSerializer<>();
		jas.configure(producerConfig(), false);
		return new DefaultKafkaProducerFactory<String, BalanceLog>(producerConfig(),
				new StringSerializer(), jas);
	}

	@Bean
	public KafkaTemplate<String, BalanceLog> balanceLogKafkaTemplate(ProducerFactory<String, BalanceLog> balanceLogProducerFactory) throws Exception {
		return new KafkaTemplate<>(balanceLogProducerFactory, true); // auto-flush true, to force each message to broker.
	}	

}
