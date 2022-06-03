package dynamok.transformers;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.nsl.logical.model.CuExecutionStateDto;
import com.nsl.logical.model.TransactionMetaDataDto;
import com.nsl.logical.model.TxnData;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TransactionDto extends TransactionMetaDataDto implements Serializable {
    private Stack<CuExecutionStateDto> executionState;
    private String masterTxnId;
    private Map<String, String> masterTransactionIdRecords = new HashMap<>();
    private TxnData txnData;
    private String baseContextualId;

    public Stack<CuExecutionStateDto> getExecutionState() {
        return executionState;
    }

    public void setExecutionState(Stack<CuExecutionStateDto> executionState) {
        this.executionState = executionState;
    }

    public String getMasterTxnId() {
        return masterTxnId;
    }

    public void setMasterTxnId(String masterTxnId) {
        this.masterTxnId = masterTxnId;
    }

    public Map<String, String> getMasterTransactionIdRecords() {
        return masterTransactionIdRecords;
    }

    public void setMasterTransactionIdRecords(Map<String, String> masterTransactionIdRecords) {
        this.masterTransactionIdRecords = masterTransactionIdRecords;
    }

    public TxnData getTxnData() {
        return txnData;
    }

    public void setTxnData(TxnData txnData) {
        this.txnData = txnData;
    }

    public String getBaseContextualId() {
        return baseContextualId;
    }

    public void setBaseContextualId(String baseContextualId) {
        this.baseContextualId = baseContextualId;
    }
}
