package com.spark.bitrade.service;

import com.spark.bipay.constant.CoinType;
import com.spark.bipay.entity.Address;
import com.spark.bipay.entity.Trade;
import com.spark.bipay.entity.Transaction;
import com.spark.bipay.http.ResponseMessage;
import com.spark.bipay.http.client.BiPayClient;
import com.spark.bitrade.constant.BooleanEnum;
import com.spark.bitrade.entity.Coin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

@Service
public class BiPayService {
    @Autowired(required = false)
    private BiPayClient biPayClient;
    @Value("${bipay.notify-host:}")
    private String host;
    @Value("#{'${bipay.supported-coins:}'.split(',')}")
    private List<String> supportedCoins;
    @Autowired
    private CoinService coinService;

    public CoinType convert2CoinType(Coin coin){
        if(coin.getName().equalsIgnoreCase("USDT")){
            return CoinType.Bitcoin;
        }
        else if(coin.getIsErcToken() == BooleanEnum.IS_TRUE){
            return CoinType.Ethereum;
        }
        else return CoinType.valueOf(coin.getName());
    }

    public Coin convert2Coin(Trade trade){
        CoinType coinType = CoinType.codeOf(Integer.parseInt(trade.getMainCoinType()));
        Coin coin = null;
        if(!trade.getMainCoinType().equalsIgnoreCase(trade.getCoinType())){
            //mainCodeType 不等于 coinType表示币种为代币
            if(trade.getCoinType().equalsIgnoreCase("31")){
                coin = coinService.findByUnit("USDT");
            }
            else {
                coin = coinService.findByTokenAddress(trade.getCoinType());
            }
        }
        else {
            coin = coinService.findOne(coinType.name());
        }
        return coin;
    }

    public boolean isSupportedCoin(String coinName){
        return  supportedCoins!=null && supportedCoins.contains(coinName);
    }

    public boolean checkSystemAddress(String address) {
        boolean fSystemAddress = false;
        try {
            return biPayClient.checkAddress(address);
        }catch (Exception e) {
            e.printStackTrace();
        }
        return fSystemAddress;
    }

    /**
     * 创建币种地址
     * @param coinType
     * @return
     */
    public Address createCoinAddress(CoinType coinType){
        String callbackUrl = host + "/bipay/notify";
        String addressAlias = ""; // 地址别名，便于在优盾钱包端的显示
        String udunWalletId = "";   // 指定地址由哪个钱包进行生成，为空默认从主钱包生成地址
        try {
            ResponseMessage<Address> resp =  biPayClient.createCoinAddress(coinType.getCode(), callbackUrl, addressAlias, udunWalletId);
            return  resp.getData();
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }


    public ResponseMessage<String> transfer(String orderId, BigDecimal amount,CoinType coinType,String subCoinType,String address, String memo){
        String callbackUrl = host + "/bipay/notify";
        try {
            ResponseMessage<String> resp =  biPayClient.transfer(orderId,amount,coinType.getCode(),subCoinType,address,callbackUrl,memo);
            return resp;
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return ResponseMessage.error("提交转币失败");
    }

    public List<Transaction> queryTransaction() throws Exception {
//        return biPayClient.queryTransaction();
        return null;
    }
}