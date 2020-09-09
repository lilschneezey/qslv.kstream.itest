package qslv.kstream.itest;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import qslv.common.kafka.ResponseMessage;
import qslv.common.kafka.TraceableMessage;
import qslv.data.Account;
import qslv.data.BalanceLog;
import qslv.data.OverdraftInstruction;
import qslv.kstream.LoggedTransaction;
import qslv.kstream.PostingResponse;
import qslv.kstream.PostingRequest;
import qslv.kstream.ReservationRequest;
import qslv.kstream.workflow.WorkflowMessage;
import qslv.util.EnableQuickSilver;
import qslv.util.Random;

@SpringBootTest
@EnableQuickSilver
class Itest_ReservationProcesser {

	@Autowired
	ConfigProperties config;
	@Autowired
	KafkaProducerDao kafkaProducerDao;

	// Response Queue
	@Autowired ArrayBlockingQueue<ResponseMessage<PostingRequest,PostingResponse>> responseExchangeQueue;
	@Autowired ArrayBlockingQueue<TraceableMessage<WorkflowMessage>> reservationMatchExchangeQueue;
	@Autowired ArrayBlockingQueue<TraceableMessage<WorkflowMessage>> transactionProcessorExchangeQueue;
	@Autowired ArrayBlockingQueue<LoggedTransaction> reservationByUuidExchangeQueue;
	@Autowired ArrayBlockingQueue<TraceableMessage<LoggedTransaction>> loggedTransactionExchangeQueue;
	@Autowired ArrayBlockingQueue<BalanceLog> balanceLogExchangeQueue;

	public static String TEST_TAXONOMY_ID = "9.9.9.9.9";
	public static String VALID_STATUS = "EF";
	public static String INVALID_STATUS = "CL";
	public static String JSON_DATA = "{\"value\": 234934}";

	public void drain_queues() {
		reservationMatchExchangeQueue.clear();
		transactionProcessorExchangeQueue.clear();
		responseExchangeQueue.clear();
		reservationByUuidExchangeQueue.clear();
		loggedTransactionExchangeQueue.clear();
		balanceLogExchangeQueue.clear();
	}

	//TODO: basic validation conditions, OD variations
	//TODO: balanceLog validation
	
	@Test
	void testReservation_success() throws Exception {
		drain_queues();

		long starting_balance = 9999L;
		long transaction_amount = -4444L;
		long expected_balance = starting_balance + transaction_amount;

		// - setup --------------------
		Account account = randomAccount(true);
		OverdraftInstruction overdraft = randomOverdraft(account.getAccountNumber(), true, true);
		kafkaProducerDao.produceAccount(account);
		kafkaProducerDao.produceOverdraft(overdraft);
		resetBalance(account.getAccountNumber(), starting_balance);

		// - prepare --------------------
		ReservationRequest reservationRequest = setupRequest(account, transaction_amount);
		TraceableMessage<PostingRequest> traceable = setupTraceable(new PostingRequest(reservationRequest));

		// - execute ------------------
		kafkaProducerDao.produceRequestMessage(traceable);
		ResponseMessage<PostingRequest, PostingResponse> response = responseExchangeQueue.take();

		// - verify ------------------
		assertNotNull(response);
		verifyTraceableMessage(traceable, response);
		assertNotNull(response.getMessageCompletionTime());
		assertEquals(ResponseMessage.SUCCESS, response.getStatus());
		assertNull(response.getErrorMessage());

		assertNotNull(response.getRequest());
		assertNotNull(response.getRequest().getReservationRequest());
		verifyRequest(reservationRequest, response.getRequest().getReservationRequest());

		assertNotNull(response.getResponse());

		assertEquals(1, response.getResponse().getTransactions().size());

		LoggedTransaction transaction = response.getResponse().getTransactions().get(0);
		assertEquals(reservationRequest.getAccountNumber(), transaction.getAccountNumber());
		assertNull(reservationRequest.getDebitCardNumber(), transaction.getDebitCardNumber());
		assertEquals(reservationRequest.getRequestUuid(), transaction.getRequestUuid());
		assertNull(transaction.getReservationUuid());
		assertEquals(expected_balance, transaction.getRunningBalanceAmount());
		assertEquals(transaction_amount, transaction.getTransactionAmount());
		assertEquals(reservationRequest.getJsonMetaData(), transaction.getTransactionMetaDataJson());
		assertNotNull(transaction.getTransactionTime());
		assertEquals(LoggedTransaction.RESERVATION, transaction.getTransactionTypeCode());
		assertNotNull(transaction.getTransactionUuid());

		TraceableMessage<LoggedTransaction> tracedTransaction = loggedTransactionExchangeQueue.take();
		verifyTraceableMessage(traceable, tracedTransaction);
		verifyTransactions(transaction, tracedTransaction.getPayload());
		
		transactionProcessorExchangeQueue.take();
		reservationByUuidExchangeQueue.take();
	}

	@Test
	void testReservation_NSF() throws Exception {
		drain_queues();

		long starting_balance = 3333L;
		long transaction_amount = -4444L;

		// - setup --------------------
		Account account = randomAccount(true);
		kafkaProducerDao.produceAccount(account);
		resetBalance(account.getAccountNumber(), starting_balance);

		// - prepare --------------------
		ReservationRequest reservationRequest = setupRequest(account, transaction_amount);
		TraceableMessage<PostingRequest> traceable = setupTraceable(new PostingRequest(reservationRequest));

		// - execute ------------------
		kafkaProducerDao.produceRequestMessage(traceable);
		ResponseMessage<PostingRequest, PostingResponse> response = responseExchangeQueue.take();

		// - verify ------------------
		assertNotNull(response);
		verifyTraceableMessage(traceable, response);
		assertNotNull(response.getMessageCompletionTime());
		assertEquals(ResponseMessage.INSUFFICIENT_FUNDS, response.getStatus());
		assertTrue(response.getErrorMessage().contains("Insufficient funds"));

		assertNotNull(response.getRequest());
		assertNotNull(response.getRequest().getReservationRequest());
		verifyRequest(reservationRequest, response.getRequest().getReservationRequest());

		assertNotNull(response.getResponse());

		assertEquals(1, response.getResponse().getTransactions().size());

		LoggedTransaction transaction = response.getResponse().getTransactions().get(0);
		assertEquals(reservationRequest.getAccountNumber(), transaction.getAccountNumber());
		assertNull(reservationRequest.getDebitCardNumber(), transaction.getDebitCardNumber());
		assertEquals(reservationRequest.getRequestUuid(), transaction.getRequestUuid());
		assertNull(transaction.getReservationUuid());
		assertEquals(starting_balance, transaction.getRunningBalanceAmount());
		assertEquals(transaction_amount, transaction.getTransactionAmount());
		assertEquals(reservationRequest.getJsonMetaData(), transaction.getTransactionMetaDataJson());
		assertNotNull(transaction.getTransactionTime());
		assertEquals(LoggedTransaction.REJECTED_TRANSACTION, transaction.getTransactionTypeCode());
		assertNotNull(transaction.getTransactionUuid());

		TraceableMessage<LoggedTransaction> tracedTransaction = loggedTransactionExchangeQueue.take();
		verifyTraceableMessage(traceable, tracedTransaction);
		verifyTransactions(transaction, tracedTransaction.getPayload());

		transactionProcessorExchangeQueue.take();
	}
	
	@Test
	void testReservation_ODprotection() throws Exception {
		drain_queues();

		long starting_balance = 3333L;
		long startingODbalance = 9999L;
		long transaction_amount = -4444L;
		long expected_balance = starting_balance;
		long expectedODbalance = startingODbalance + transaction_amount;

		// - setup --------------------
		Account account = randomAccount(true);
		OverdraftInstruction overdraft = randomOverdraft(account.getAccountNumber(), true, true);
		kafkaProducerDao.produceAccount(account);
		kafkaProducerDao.produceOverdraft(overdraft);
		resetBalance(account.getAccountNumber(), starting_balance);
		resetBalance(overdraft.getOverdraftAccount().getAccountNumber(), startingODbalance);

		// - prepare --------------------
		ReservationRequest reservationRequest = setupRequest(account, transaction_amount);
		TraceableMessage<PostingRequest> traceable = setupTraceable(new PostingRequest(reservationRequest));

		// - execute ------------------
		kafkaProducerDao.produceRequestMessage(traceable);
		ResponseMessage<PostingRequest, PostingResponse> response = responseExchangeQueue.take();

		// - verify ------------------
		assertNotNull(response);
		verifyTraceableMessage(traceable, response);
		assertNotNull(response.getMessageCompletionTime());
		assertEquals(ResponseMessage.SUCCESS, response.getStatus());
		assertNull(response.getErrorMessage());

		assertNotNull(response.getRequest());
		assertNotNull(response.getRequest().getReservationRequest());
		verifyRequest(reservationRequest, response.getRequest().getReservationRequest());

		assertNotNull(response.getResponse());

		assertEquals(2, response.getResponse().getTransactions().size());

		LoggedTransaction rejection = response.getResponse().getTransactions().get(0);
		assertEquals(reservationRequest.getAccountNumber(), rejection.getAccountNumber());
		assertNull(reservationRequest.getDebitCardNumber(), rejection.getDebitCardNumber());
		assertEquals(reservationRequest.getRequestUuid(), rejection.getRequestUuid());
		assertNull(rejection.getReservationUuid());
		assertEquals(expected_balance, rejection.getRunningBalanceAmount());
		assertEquals(transaction_amount, rejection.getTransactionAmount());
		assertEquals(reservationRequest.getJsonMetaData(), rejection.getTransactionMetaDataJson());
		assertNotNull(rejection.getTransactionTime());
		assertEquals(LoggedTransaction.REJECTED_TRANSACTION, rejection.getTransactionTypeCode());
		assertNotNull(rejection.getTransactionUuid());
		
		LoggedTransaction reservation = response.getResponse().getTransactions().get(1);
		assertEquals(overdraft.getOverdraftAccount().getAccountNumber(), reservation.getAccountNumber());
		assertNull(reservationRequest.getDebitCardNumber(), reservation.getDebitCardNumber());
		assertEquals(reservationRequest.getRequestUuid(), reservation.getRequestUuid());
		assertNull(reservation.getReservationUuid());
		assertEquals(expectedODbalance, reservation.getRunningBalanceAmount());
		assertEquals(transaction_amount, reservation.getTransactionAmount());
		assertEquals(reservationRequest.getJsonMetaData(), reservation.getTransactionMetaDataJson());
		assertNotNull(reservation.getTransactionTime());
		assertEquals(LoggedTransaction.RESERVATION, reservation.getTransactionTypeCode());
		assertNotNull(reservation.getTransactionUuid());
		
		TraceableMessage<LoggedTransaction> tracedRejection = loggedTransactionExchangeQueue.take();
		verifyTraceableMessage(traceable, tracedRejection);
		verifyTransactions(rejection, tracedRejection.getPayload());
		
		TraceableMessage<LoggedTransaction> tracedReservation = loggedTransactionExchangeQueue.take();
		verifyTraceableMessage(traceable, tracedReservation);
		verifyTransactions(reservation, tracedReservation.getPayload());
		
		transactionProcessorExchangeQueue.take();
		transactionProcessorExchangeQueue.take();
		reservationByUuidExchangeQueue.take();
	}
	
	private void resetBalance(String accountNumber, long balance) {
		BalanceLog log = new BalanceLog();
		log.setAccountNumber(accountNumber);
		log.setLastTransaction(UUID.randomUUID());
		log.setBalance(balance);
		kafkaProducerDao.produceBalanceLog(log);
	}

	Account randomAccount(boolean valid) {
		Account account = new Account();
		account.setAccountLifeCycleStatus(valid ? "EF" : "CL");
		account.setAccountNumber(Random.randomDigits(12));
		return account;
	}

	OverdraftInstruction randomOverdraft(String accountNumber, boolean valid, boolean accountValid) {
		OverdraftInstruction od = new OverdraftInstruction();
		od.setAccountNumber(accountNumber);
		od.setEffectiveStart(LocalDateTime.now().minusMonths(12));
		od.setEffectiveEnd(LocalDateTime.now().plusMonths(12));
		od.setInstructionLifecycleStatus(valid ? "EF" : "CL");
		od.setOverdraftAccount(randomAccount(accountValid));
		return od;
	}

	ReservationRequest setupRequest(Account account, long amount) {
		ReservationRequest request = new ReservationRequest();
		request.setRequestUuid(UUID.randomUUID());
		request.setAccountNumber(account.getAccountNumber());
		request.setDebitCardNumber(null);
		request.setTransactionAmount(amount);
		request.setJsonMetaData(JSON_DATA);
		return request;
	}

	<T> TraceableMessage<T> setupTraceable(T payload) {
		TraceableMessage<T> traceable = new TraceableMessage<>();
		traceable.setProducerAit(config.getAitid());
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId("sdfsdfsdf");
		traceable.setPayload(payload);
		traceable.setMessageCreationTime(LocalDateTime.now());
		return traceable;
	}

	void verifyRequest(ReservationRequest expected, ReservationRequest actual) {
		assertEquals(expected.getAccountNumber(), actual.getAccountNumber());
		assertEquals(expected.getDebitCardNumber(), actual.getDebitCardNumber());
		assertEquals(expected.getJsonMetaData(), actual.getJsonMetaData());
		assertEquals(expected.getRequestUuid(), actual.getRequestUuid());
		assertEquals(expected.getTransactionAmount(), actual.getTransactionAmount());
	}

	void verifyTraceableMessage(TraceableMessage<?> expected, ResponseMessage<?, ?> actual) {
		assertEquals(expected.getBusinessTaxonomyId(), actual.getBusinessTaxonomyId());
		assertEquals(expected.getCorrelationId(), actual.getCorrelationId());
		assertEquals(expected.getMessageCreationTime(), actual.getMessageCreationTime());
		assertEquals(expected.getProducerAit(), actual.getProducerAit());
	}

	void verifyTraceableMessage(TraceableMessage<?> expected, TraceableMessage<?> actual) {
		assertEquals(expected.getBusinessTaxonomyId(), actual.getBusinessTaxonomyId());
		assertEquals(expected.getCorrelationId(), actual.getCorrelationId());
		assertEquals(expected.getMessageCreationTime(), actual.getMessageCreationTime());
		assertEquals(expected.getProducerAit(), actual.getProducerAit());
	}

	private void verifyTransactions(LoggedTransaction reservation, LoggedTransaction ttran) {
		assertEquals(reservation.getAccountNumber(), ttran.getAccountNumber());
		assertEquals(reservation.getDebitCardNumber(), ttran.getDebitCardNumber());
		assertEquals(reservation.getRequestUuid(), ttran.getRequestUuid());
		assertEquals(reservation.getReservationUuid(), ttran.getReservationUuid());
		assertEquals(reservation.getRunningBalanceAmount(), ttran.getRunningBalanceAmount());
		assertEquals(reservation.getTransactionAmount(), ttran.getTransactionAmount());
		assertEquals(reservation.getTransactionMetaDataJson(), ttran.getTransactionMetaDataJson());
		assertEquals(reservation.getTransactionTime(), ttran.getTransactionTime());
		assertEquals(reservation.getTransactionTypeCode(), ttran.getTransactionTypeCode());
		assertEquals(reservation.getTransactionUuid(), ttran.getTransactionUuid());
	}

}
