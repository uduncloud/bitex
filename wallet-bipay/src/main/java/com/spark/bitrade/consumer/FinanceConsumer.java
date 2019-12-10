package com.spark.bitrade.consumer;

import com.alibaba.druid.util.StringUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.spark.bipay.constant.CoinType;
import com.spark.bitrade.constant.BooleanEnum;
import com.spark.bitrade.entity.Coin;
import com.spark.bitrade.entity.HotTransferRecord;
import com.spark.bitrade.entity.WithdrawRecord;
import com.spark.bitrade.service.*;
import com.spark.bitrade.util.MessageResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

import static com.spark.bitrade.constant.BooleanEnum.IS_TRUE;

@Component
public class FinanceConsumer {
    private Logger logger = LoggerFactory.getLogger(FinanceConsumer.class);
    @Autowired
    private CoinService coinService;
    @Autowired
    private MemberWalletService walletService;
    @Autowired
    private RestTemplate restTemplate;
    @Autowired
    private WithdrawRecordService withdrawRecordService;
    @Autowired
    private HotTransferRecordService hotTransferRecordService;
    @Autowired
    private BiPayService biPayService;

    /**
     * 处理充值消息，key值为币种的名称（注意是全称，如Bitcoin）
     *
     * @param record
     */
    @KafkaListener(topics = {"deposit"})
    public void handleDeposit(ConsumerRecord<String, String> record) {
        logger.info("topic={},accessKey={},value={}", record.topic(), record.key(), record.value());
        if (StringUtils.isEmpty(record.value())) {
            return;
        }
        JSONObject json = JSON.parseObject(record.value());
        if (json == null) {
            return;
        }
        BigDecimal amount = json.getBigDecimal("amount");
        String txid = json.getString("txid");
        String address = json.getString("address");
        Coin coin = coinService.findOne(record.key());
        logger.info("coin={}", coin);
        if (coin != null
                && walletService.findDeposit(address, txid) == null
                && amount.compareTo(coin.getMinRechargeAmount()) >= 0) {
            MessageResult mr = walletService.recharge(coin, address, amount, txid);
            logger.info("wallet recharge result:{}", mr);
        }
    }

    /**
     * 处理提交请求,调用钱包rpc，自动转账
     *
     * @param record
     */
    @KafkaListener(topics = {"withdraw"})
    public void handleWithdraw(ConsumerRecord<String, String> record) {
        logger.info("topic={},accessKey={},value={}", record.topic(), record.key(), record.value());
        if (StringUtils.isEmpty(record.value())) {
            return;
        }
        JSONObject json = JSON.parseObject(record.value());
        Long withdrawId = json.getLong("withdrawId");
        WithdrawRecord withdrawRecord=withdrawRecordService.findOne(withdrawId);
        try {
            String serviceName = "SERVICE-RPC-" + record.key().toUpperCase();
            String url = "http://" + serviceName + "/rpc/withdraw?address={1}&amount={2}&fee={3}&remark={4}&sync=false&withdrawId="+withdrawId;
            Coin coin = coinService.findByUnit(record.key());
            logger.info("coin = {}",coin.toString());
            if(biPayService.isSupportedCoin(coin.getUnit())){
                //审核通过后发送转账申请
                CoinType coinType = biPayService.convert2CoinType(coin);
                String subCoinType = String.valueOf(coinType.getCode());
                if(coin.getIsErcToken() == IS_TRUE){
                    subCoinType = coin.getTokenAddress();
                }
                else if(coin.getName().equalsIgnoreCase("USDT")){
                    subCoinType = "31";
                }
                if (biPayService.checkSystemAddress(json.getString("address"))) {
                    withdrawRecordService.withdrawSuccessForSystem(withdrawId);
                }else {
                    //发起转账申请
                    biPayService.transfer(String.valueOf(withdrawId), withdrawRecord.getArrivedAmount(), coinType, subCoinType, withdrawRecord.getAddress(),withdrawRecord.getRemark());
                }
            }else if (coin != null && coin.getCanAutoWithdraw() == IS_TRUE) {
                BigDecimal minerFee = coin.getMinerFee();
                String remark = json.containsKey("remark") ? json.getString("remark") : "";
                MessageResult result = restTemplate.getForObject(url,
                        MessageResult.class, json.getString("address"), json.getBigDecimal("arriveAmount"), minerFee,remark);
                logger.info("result = {}", result);
                if (result.getCode() == 0 && result.getData() != null) {
                    //处理成功,data为txid，更新业务订单
                    String txid = (String) result.getData();
                    withdrawRecordService.withdrawSuccess(withdrawId, txid);
                }
                else if(result.getCode() == 200){
                    //提币转账中，等待通知
                    logger.info("====================== 提币转为异步转账 ==================================");
                    withdrawRecordService.withdrawTransfering(withdrawId);
                }
                else {
                    logger.info("====================== 自动转账失败，转为人工处理 ==================================");
                    //自动转账失败，转为人工处理
                    withdrawRecordService.autoWithdrawFail(withdrawId);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("auto withdraw failed,error={}", e.getMessage());
            //自动转账失败，转为人工处理
            withdrawRecordService.autoWithdrawFail(withdrawId);
        }
    }


    /**
     * 处理提交请求,调用钱包rpc，自动转账
     *
     * @param record
     */
//    @KafkaListener(topics = {"withdraw"})
//    public void handleWithdraw(ConsumerRecord<String, String> record) {
//        logger.info("topic={},accessKey={},value={}", record.topic(), record.key(), record.value());
//        if (StringUtils.isEmpty(record.value())) {
//            return;
//        }
//        JSONObject json = JSON.parseObject(record.value());
//        Long withdrawId = json.getLong("withdrawId");
//        try {
//            String serviceName = "SERVICE-RPC-" + record.key().toUpperCase();
//            String url = "http://" + serviceName + "/rpc/withdraw?address={1}&amount={2}&fee={3}&remark={4}&sync=false&withdrawId="+withdrawId;
//            Coin coin = coinService.findByUnit(record.key());
//            logger.info("coin = {}",coin.toString());
//            if (coin != null && coin.getCanAutoWithdraw() == BooleanEnum.IS_TRUE) {
//                BigDecimal minerFee = coin.getMinerFee();
//                String remark = json.containsKey("remark") ? json.getString("remark") : "";
//                MessageResult result = restTemplate.getForObject(url,
//                        MessageResult.class, json.getString("address"), json.getBigDecimal("arriveAmount"), minerFee,remark);
//                logger.info("result = {}", result);
//                if (result.getCode() == 0 && result.getData() != null) {
//                    //处理成功,data为txid，更新业务订单
//                    String txid = (String) result.getData();
//                    withdrawRecordService.withdrawSuccess(withdrawId, txid);
//                }
//                else if(result.getCode() == 200){
//                    //提币转账中，等待通知
//                    logger.info("====================== 提币转为异步转账 ==================================");
//                    withdrawRecordService.withdrawTransfering(withdrawId);
//                }
//                else {
//                    logger.info("====================== 自动转账失败，转为人工处理 ==================================");
//                    //自动转账失败，转为人工处理
//                    withdrawRecordService.autoWithdrawFail(withdrawId);
//                }
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//            logger.error("auto withdraw failed,error={}", e.getMessage());
//            //自动转账失败，转为人工处理
//            withdrawRecordService.autoWithdrawFail(withdrawId);
//        }
//    }

    /**
     * 异步打钱后返回状态
     * @param record
     */
    @KafkaListener(topics = {"withdraw-notify"})
    public void withdrawNotify(ConsumerRecord<String, String> record){
        logger.info("topic={},accessKey={},value={}", record.topic(), record.key(), record.value());
        if (StringUtils.isEmpty(record.value())) {
            return;
        }
        JSONObject json = JSON.parseObject(record.value());
        Long withdrawId = json.getLong("withdrawId");
        WithdrawRecord withdrawRecord=withdrawRecordService.findOne(withdrawId);
        if(withdrawRecord==null){
            return;
        }
        String txid=json.getString("txid");
        int status=json.getInteger("status");
        //转账失败，状态变回等待放币
        if(status==1){
            withdrawRecordService.withdrawSuccess(withdrawId, txid);
        }else{
            String errorMsg=madeMsg(status,withdrawRecord.getErrorMsg());
            withdrawRecord.setErrorMsg(errorMsg);
            withdrawRecordService.save(withdrawRecord);
        }
    }

    /**
     * 异步转入冷钱包后返回状态
     * @param record
     */
    @KafkaListener(topics = {"transfer-notify"})
    public void transferNotify(ConsumerRecord<String, String> record){
        logger.info("topic={},accessKey={},value={}", record.topic(), record.key(), record.value());
        if (StringUtils.isEmpty(record.value())) {
            return;
        }
        JSONObject json = JSON.parseObject(record.value());
        int status=json.getInteger("status");
        Long transferId = json.getLong("withdrawId");
        HotTransferRecord hotTransferRecord=hotTransferRecordService.findById(transferId);
        if(hotTransferRecord==null){
            return;
        }
        if(status==1){
            Double amount=json.getDouble("amount");
            hotTransferRecord.setRealAmount(hotTransferRecord.getRealAmount().add(new BigDecimal(amount)));
            String txid=json.getString("txid");
            if(StringUtils.isEmpty(hotTransferRecord.getTransactionNumber())){
                hotTransferRecord.setTransactionNumber(txid);
            }else{
                hotTransferRecord.setTransactionNumber(hotTransferRecord.getTransactionNumber()+","+txid);
            }
            hotTransferRecordService.save(hotTransferRecord);
        }else{
            String errorMsg=madeMsg(status,hotTransferRecord.getErrorMsg());
            hotTransferRecord.setErrorMsg(errorMsg);
            hotTransferRecordService.save(hotTransferRecord);
        }
    }

    private String madeMsg(int status,String oldMsg){
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String errorMsg=sdf.format(new Date());
        if(!StringUtils.isEmpty(oldMsg)){
            errorMsg=oldMsg+errorMsg;
        }
        if(status==0){
            errorMsg=errorMsg+":转账失败，检查状态超过1000次，提币钱包余额不足或区块钱包同步数据落后\n";
        }else if(status==2){
            errorMsg=errorMsg+":创建转账失败，地址解析错误\n";
        }else if (status==3){
            errorMsg=errorMsg+":创建转账失败，提币钱包余额不足或矿工费不足\n";
        }else if(status==4){
            errorMsg=errorMsg+":转账失败，gaslimit过小\n";
        }
        return errorMsg;
    }
}
