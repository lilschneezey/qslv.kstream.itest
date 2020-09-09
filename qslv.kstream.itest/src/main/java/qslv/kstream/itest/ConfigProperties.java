package qslv.kstream.itest;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "qslv")
public class ConfigProperties {
	private String aitid = "27834";
	
	private String requestTopic;
	private String responseTopic;
	private String enhancedRequestTopic;
	private String matchReservationTopic;
	private String accountTopic;
	private String overdraftTopic;
	private String loggedTransactionTopic;
	private String reservationByUuidTopic;
	private String balanceLogStateStoreTopic;
	
	private String kafkaConsumerPropertiesPath;
	private String kafkaProducerPropertiesPath;
	
	public String getAitid() {
		return aitid;
	}
	public void setAitid(String aitid) {
		this.aitid = aitid;
	}
	public String getRequestTopic() {
		return requestTopic;
	}
	public void setRequestTopic(String requestTopic) {
		this.requestTopic = requestTopic;
	}
	public String getResponseTopic() {
		return responseTopic;
	}
	public void setResponseTopic(String responseTopic) {
		this.responseTopic = responseTopic;
	}
	public String getEnhancedRequestTopic() {
		return enhancedRequestTopic;
	}
	public void setEnhancedRequestTopic(String enhancedRequestTopic) {
		this.enhancedRequestTopic = enhancedRequestTopic;
	}
	public String getMatchReservationTopic() {
		return matchReservationTopic;
	}
	public void setMatchReservationTopic(String matchReservationTopic) {
		this.matchReservationTopic = matchReservationTopic;
	}
	public String getAccountTopic() {
		return accountTopic;
	}
	public void setAccountTopic(String accountTopic) {
		this.accountTopic = accountTopic;
	}
	public String getOverdraftTopic() {
		return overdraftTopic;
	}
	public void setOverdraftTopic(String overdraftTopic) {
		this.overdraftTopic = overdraftTopic;
	}
	public String getLoggedTransactionTopic() {
		return loggedTransactionTopic;
	}
	public void setLoggedTransactionTopic(String loggedTransactionTopic) {
		this.loggedTransactionTopic = loggedTransactionTopic;
	}
	public String getReservationByUuidTopic() {
		return reservationByUuidTopic;
	}
	public void setReservationByUuidTopic(String reservationByUuidTopic) {
		this.reservationByUuidTopic = reservationByUuidTopic;
	}
	public String getBalanceLogStateStoreTopic() {
		return balanceLogStateStoreTopic;
	}
	public void setBalanceLogStateStoreTopic(String balanceLogStateStoreTopic) {
		this.balanceLogStateStoreTopic = balanceLogStateStoreTopic;
	}
	public String getKafkaConsumerPropertiesPath() {
		return kafkaConsumerPropertiesPath;
	}
	public void setKafkaConsumerPropertiesPath(String kafkaConsumerPropertiesPath) {
		this.kafkaConsumerPropertiesPath = kafkaConsumerPropertiesPath;
	}
	public String getKafkaProducerPropertiesPath() {
		return kafkaProducerPropertiesPath;
	}
	public void setKafkaProducerPropertiesPath(String kafkaProducerPropertiesPath) {
		this.kafkaProducerPropertiesPath = kafkaProducerPropertiesPath;
	}

}
