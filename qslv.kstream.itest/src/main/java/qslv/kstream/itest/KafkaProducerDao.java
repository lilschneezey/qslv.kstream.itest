package qslv.kstream.itest;


import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import qslv.common.kafka.TraceableMessage;
import qslv.data.Account;
import qslv.data.BalanceLog;
import qslv.data.OverdraftInstruction;
import qslv.kstream.PostingRequest;

@Component
public class KafkaProducerDao {
	private static final Logger log = LoggerFactory.getLogger(KafkaProducerDao.class);

	@Autowired
	private ConfigProperties config;
	
	@Autowired
	public KafkaTemplate<String, TraceableMessage<PostingRequest>> requestTemplate;
	@Autowired
	public KafkaTemplate<String, Account> accountTemplate;
	@Autowired
	public KafkaTemplate<String, OverdraftInstruction> overdraftTemplate;
	@Autowired
	public KafkaTemplate<String, BalanceLog> balanceLogTemplate;
	
	public void produceRequestMessage(TraceableMessage<PostingRequest> request) throws ResponseStatusException {
		String key = "null";

		if ( request.getPayload().hasCancelReservationRequest() )
			key = request.getPayload().getCancelReservationRequest().getAccountNumber();
		
		else if ( request.getPayload().hasReservationRequest() )
			key = request.getPayload().getReservationRequest().getAccountNumber();
		
		else if ( request.getPayload().hasCommitReservationRequest() )
			key = request.getPayload().getCommitReservationRequest().getAccountNumber();
		
		else if ( request.getPayload().hasTransactionRequest() )
			key = request.getPayload().getTransactionRequest().getAccountNumber();
		
		else if ( request.getPayload().hasTransferRequest() )
			key = request.getPayload().getTransferRequest().getTransferFromAccountNumber();

		try {
			// retry handled internally by kafka using retries & retry.backoff.ms in properties file
			ProducerRecord<String ,TraceableMessage<PostingRequest>> record = 
					requestTemplate.send(config.getRequestTopic(), key, request)
					.get()
					.getProducerRecord();
			log.debug("Kakfa Produce {}", record.value().getPayload());
		} catch ( Exception ex) {
			log.debug(ex.getLocalizedMessage());
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Kafka Producer failure", ex);
		}

	}
	
	public void produceAccount(Account account) throws ResponseStatusException {
		try {
			// retry handled internally by kafka using retries & retry.backoff.ms in properties file
			ProducerRecord<String ,Account> record = 
					accountTemplate.send(config.getAccountTopic(),account.getAccountNumber(), account)
				.get().getProducerRecord();
			log.debug("Kakfa Produce {}", record.value());
		} catch ( Exception ex) {
			log.debug(ex.getLocalizedMessage());
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Kafka Producer failure", ex);
		}
	}
	
	public void produceOverdraft(OverdraftInstruction overdraft) throws ResponseStatusException {
		try {
			// retry handled internally by kafka using retries & retry.backoff.ms in properties file
			ProducerRecord<String ,OverdraftInstruction> record = 
					overdraftTemplate.send(config.getOverdraftTopic(),overdraft.getAccountNumber(), overdraft)
				.get().getProducerRecord();
			log.debug("Kakfa Produce {}", record.value());
		} catch ( Exception ex) {
			log.debug(ex.getLocalizedMessage());
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Kafka Producer failure", ex);
		}
	}


	public void produceBalanceLog(BalanceLog balanceLog) {
		try {
			// retry handled internally by kafka using retries & retry.backoff.ms in properties file
			ProducerRecord<String, BalanceLog> record = 
					balanceLogTemplate.send(config.getBalanceLogStateStoreTopic(), balanceLog.getAccountNumber(), balanceLog)
				.get().getProducerRecord();
			log.debug("Kakfa Produce {}", record.value());
		} catch ( Exception ex) {
			log.debug(ex.getLocalizedMessage());
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Kafka Producer failure", ex);
		}
	}

}
