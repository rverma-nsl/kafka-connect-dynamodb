package dynamok.sink;

import java.util.Map;

/**
 * Project: kafka-connect-dynamodb Author: shivamsharma Date: 1/11/18.
 */
public class DynamoConnectMetaData {
  private String conditionalExpression;
  private Map<String, Object> conditionalValueMap;

  public String getConditionalExpression() {
    return conditionalExpression;
  }

  public void setConditionalExpression(String conditionalExpression) {
    this.conditionalExpression = conditionalExpression;
  }

  public Map<String, Object> getConditionalValueMap() {
    return conditionalValueMap;
  }

  public void setConditionalValueMap(Map<String, Object> conditionalValueMap) {
    this.conditionalValueMap = conditionalValueMap;
  }
}
