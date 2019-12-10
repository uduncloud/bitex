package com.spark.bitrade.service;

import com.querydsl.core.types.*;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.jpa.impl.JPAQuery;
import com.spark.bipay.constant.CoinType;
import com.spark.bitrade.constant.BooleanEnum;
import com.spark.bitrade.constant.PageModel;
import com.spark.bitrade.constant.TransactionType;
import com.spark.bitrade.constant.WithdrawStatus;
import com.spark.bitrade.dao.WithdrawRecordDao;
import com.spark.bitrade.entity.*;
import com.spark.bitrade.pagination.Criteria;
import com.spark.bitrade.pagination.PageListMapResult;
import com.spark.bitrade.pagination.PageResult;
import com.spark.bitrade.pagination.Restrictions;
import com.spark.bitrade.service.Base.BaseService;
import com.spark.bitrade.util.DateUtil;
import com.spark.bitrade.vo.WithdrawRecordVO;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.spark.bitrade.constant.BooleanEnum.IS_FALSE;
import static com.spark.bitrade.constant.BooleanEnum.IS_TRUE;
import static com.spark.bitrade.constant.WithdrawStatus.*;
import static com.spark.bitrade.entity.QWithdrawRecord.withdrawRecord;
import static org.springframework.util.Assert.isTrue;
import static org.springframework.util.Assert.notNull;

/**
 * @author Zhang Jinwei
 * @date 2018年01月29日
 */
@Service
@Slf4j
public class WithdrawRecordService extends BaseService {
    private Logger logger = LoggerFactory.getLogger(WithdrawRecordService.class);
    @Autowired
    private WithdrawRecordDao withdrawApplyDao;
    @Autowired
    private MemberWalletService walletService;
    @Autowired
    private MemberTransactionService transactionService;
    @Autowired
    private BiPayService biPayService;

    public WithdrawRecord save(WithdrawRecord withdrawApply) {
        return withdrawApplyDao.save(withdrawApply);
    }

    public List<WithdrawRecord> findAll() {
        return withdrawApplyDao.findAll();
    }

    public WithdrawRecord findOne(Long id) {
        return withdrawApplyDao.findOne(id);
    }

    /**
     * 条件查询对象
     *
     * @param predicateList
     * @param pageNo
     * @param pageSize
     * @return
     */
    @Transactional(readOnly = true)
    public PageResult<WithdrawRecord> query(List<Predicate> predicateList, Integer pageNo, Integer pageSize) {
        List<WithdrawRecord> list;
        JPAQuery<WithdrawRecord> jpaQuery = queryFactory.selectFrom(withdrawRecord);
        if (predicateList != null)
            jpaQuery.where(predicateList.toArray(new Predicate[predicateList.size()]));
        if (pageNo != null && pageSize != null)
            list = jpaQuery.offset((pageNo - 1) * pageSize).limit(pageSize).fetch();
        else
            list = jpaQuery.fetch();
        return new PageResult<>(list, jpaQuery.fetchCount());
    }

    @Transactional(readOnly = true)
    public void test() {
        //查询字段
        List<Expression> expressions = new ArrayList<>();
        expressions.add(QWithdrawRecord.withdrawRecord.memberId.as("memberId"));
        //查询表
        List<EntityPath> entityPaths = new ArrayList<>();
        entityPaths.add(QWithdrawRecord.withdrawRecord);
        entityPaths.add(QMember.member);
        //查询条件
        List<Predicate> predicates = new ArrayList<>();
        predicates.add(QWithdrawRecord.withdrawRecord.memberId.eq(QMember.member.id));
        //排序
        List<OrderSpecifier> orderSpecifierList = new ArrayList<>();
        orderSpecifierList.add(QWithdrawRecord.withdrawRecord.id.desc());
        PageListMapResult pageListMapResult = super.queryDslForPageListResult(expressions, entityPaths, predicates, orderSpecifierList, 1, 10);
        logger.info(pageListMapResult.toString());

    }

    /**
     * 修改 根据withdrawApply.id[]修改withdrawApply的WithdrawStatus
     *
     * @param ids    withdrawApply.id[]
     * @param status WithdrawStatus
     */
    @Transactional(rollbackFor = Exception.class)
    public void audit(Long[] ids, WithdrawStatus status) {
        WithdrawRecord withdrawRecord;
        for (Long id : ids) {
            //20	4.70000000	0	2018-02-27 17:47:37		0.30000000	0	28	0	5.00000000			GalaxyChain
            withdrawRecord = withdrawApplyDao.findOne(id);
            //确认提现申请存在
            notNull(withdrawRecord, "不存在");
            //确认订单状态是审核中
            isTrue(withdrawRecord.getStatus() == PROCESSING, "id为" + id + "不是审核状态的提现");
            //确认提现类型不是自动提现
            isTrue(withdrawRecord.getIsAuto() == IS_FALSE, "id为" + id + "不是人工审核提现");
            //审核
            if (status == FAIL) {
                //审核不通过
                MemberWallet wallet = walletService.findByCoinAndMemberId(withdrawRecord.getCoin(), withdrawRecord.getMemberId());
                notNull(wallet, "wallet null!");
                wallet.setBalance(wallet.getBalance().add(withdrawRecord.getTotalAmount()));
                wallet.setFrozenBalance(wallet.getFrozenBalance().subtract(withdrawRecord.getTotalAmount()));
                walletService.save(wallet);
            }
            else if(status == WAITING && biPayService.isSupportedCoin(withdrawRecord.getCoin().getUnit())
                    && !biPayService.checkSystemAddress(withdrawRecord.getAddress())){
                //审核通过后发送转账申请
                CoinType coinType = biPayService.convert2CoinType(withdrawRecord.getCoin());
                String subCoinType = String.valueOf(coinType.getCode());
                if(withdrawRecord.getCoin().getIsErcToken() == IS_TRUE){
                    subCoinType = withdrawRecord.getCoin().getTokenAddress();
                }
                else if(withdrawRecord.getCoin().getName().equalsIgnoreCase("USDT")){
                    subCoinType = "31";
                }
                log.info("coinType={},subCoinType={}",coinType,subCoinType);
                //发起转账申请
                biPayService.transfer(String.valueOf(withdrawRecord.getId()),withdrawRecord.getArrivedAmount(),coinType,subCoinType,withdrawRecord.getAddress(),withdrawRecord.getRemark());
            } else if (status == WAITING && biPayService.isSupportedCoin(withdrawRecord.getCoin().getUnit())
                    && biPayService.checkSystemAddress(withdrawRecord.getAddress())) {
                //假如转出地址为内部地址
                withdrawSuccessForSystem(withdrawRecord);
                return;
            }
            withdrawRecord.setStatus(status);
            withdrawRecord.setDealTime(new Date());
            withdrawApplyDao.save(withdrawRecord);
        }
    }

    /**
     * 内部转账成功
     * txid格式：system:时间戳:fromId:toId
     */
    @Transactional
    public void withdrawSuccessForSystem(WithdrawRecord record) {
        String txid = "system:"+ DateUtil.getTimeMillis();
        if (record != null) {
            record.setStatus(WithdrawStatus.SUCCESS);
            record.setDealTime(new Date());
            MemberWallet wallet = walletService.findByCoinUnitAndMemberId(record.getCoin().getUnit(), record.getMemberId());
            if (wallet != null) {
                txid += ":"+wallet.getMemberId();
                walletService.decreaseFrozen(wallet.getId(),record.getTotalAmount());
                MemberTransaction transaction = new MemberTransaction();
                transaction.setAmount(record.getTotalAmount().negate());
                transaction.setSymbol(wallet.getCoin().getUnit());
                transaction.setAddress(wallet.getAddress());
                transaction.setMemberId(wallet.getMemberId());
                transaction.setType(TransactionType.WITHDRAW);
                transaction.setFee(record.getFee().negate());
                transactionService.save(transaction);
            }

            MemberWallet toWallet = walletService.findByCoinAndAddress(record.getCoin(), record.getAddress());
            if (toWallet != null) {
                //内部转账，给to用户充币
                txid += ":"+toWallet.getMemberId();
                walletService.recharge(record.getCoin(),record.getAddress(),record.getArrivedAmount(),txid);
            }
            record.setTransactionNumber(txid);
        }
    }

    @Transactional
    public void withdrawSuccessForSystem(Long withdrawId) {
        WithdrawRecord record = withdrawApplyDao.findOne(withdrawId);
        withdrawSuccessForSystem(record);
    }

    /**
     * 提现成功处理
     *
     * @param withdrawId
     * @param txid
     */
    @Transactional
    public void withdrawSuccess(Long withdrawId, String txid) {
        WithdrawRecord record = findOne(withdrawId);
        if (record != null) {
            record.setTransactionNumber(txid);
            record.setStatus(WithdrawStatus.SUCCESS);
            MemberWallet wallet = walletService.findByCoinUnitAndMemberId(record.getCoin().getUnit(), record.getMemberId());
            if (wallet != null) {
                walletService.decreaseFrozen(wallet.getId(),record.getTotalAmount());
                MemberTransaction transaction = new MemberTransaction();
                transaction.setAmount(record.getTotalAmount().negate());
                transaction.setSymbol(wallet.getCoin().getUnit());
                transaction.setAddress(wallet.getAddress());
                transaction.setMemberId(wallet.getMemberId());
                transaction.setType(TransactionType.WITHDRAW);
                transaction.setFee(record.getFee().negate());
                transactionService.save(transaction);
            }
        }
    }

    @Transactional
    public  void updateWithrawTxid(Long withdrawId,String txid){
        WithdrawRecord record = findOne(withdrawId);
        if (record != null) {
            record.setTransactionNumber(txid);
        }
    }


    @Transactional
    public void withdrawTransfering(Long withdrawId){
        WithdrawRecord record = findOne(withdrawId);
        if (record != null) {
            record.setStatus(WithdrawStatus.TRANSFER);
        }
    }

    /**
     * 提现失败处理
     *
     * @param withdrawId
     */
    @Transactional
    public void withdrawFail(Long withdrawId) {
        WithdrawRecord record = findOne(withdrawId);
        if (record == null || record.getStatus() == WithdrawStatus.SUCCESS || record.getStatus() == WithdrawStatus.FAIL) {
            return;
        }
        MemberWallet wallet = walletService.findByCoinAndMemberId(record.getCoin(), record.getMemberId());
        if (wallet != null) {
            walletService.thawBalance(wallet,record.getTotalAmount());
            record.setStatus(WithdrawStatus.FAIL);
        }
    }

    /**
     * 自动转币失败，转为人工处理
     *
     * @param withdrawId
     */
    public void autoWithdrawFail(Long withdrawId) {
        WithdrawRecord record = findOne(withdrawId);
        if (record == null || record.getStatus() != WithdrawStatus.WAITING) {
            return;
        }
        logger.info("================  自动转币失败，转为人工处理  ========================");
        logger.info("================ setIsAuto : " + BooleanEnum.IS_FALSE.getNameCn() + "  ========================");
        logger.info("================ setStatus : " + WithdrawStatus.PROCESSING.getCnName() + "  ========================");
        record.setIsAuto(BooleanEnum.IS_FALSE);
        record.setStatus(WithdrawStatus.PROCESSING);
        withdrawApplyDao.save(record);

    }

    @Transactional(readOnly = true)
    public Page<WithdrawRecord> findAllByMemberId(Long memberId, int page, int pageSize) {
        Sort orders = Criteria.sortStatic("id.desc");
        PageRequest pageRequest = new PageRequest(page, pageSize, orders);
        Criteria<WithdrawRecord> specification = new Criteria<WithdrawRecord>();
        specification.add(Restrictions.eq("memberId", memberId, false));
        return withdrawApplyDao.findAll(specification, pageRequest);
    }

    public Page<WithdrawRecordVO> joinFind(List<Predicate> predicates, PageModel pageModel) {
        List<OrderSpecifier> orderSpecifiers = pageModel.getOrderSpecifiers();
        JPAQuery<WithdrawRecordVO> query = queryFactory.select(
                Projections.fields(WithdrawRecordVO.class,
                        QWithdrawRecord.withdrawRecord.id.as("id"),
                        QWithdrawRecord.withdrawRecord.memberId.as("memberId"),
                        QWithdrawRecord.withdrawRecord.coin.unit,
                        QMember.member.username.as("memberUsername"),
                        QMember.member.realName.as("memberRealName"),
                        QMember.member.mobilePhone.as("phone"),
                        QMember.member.email,
                        QWithdrawRecord.withdrawRecord.dealTime.as("dealTime"),
                        QWithdrawRecord.withdrawRecord.totalAmount.as("totalAmount"),
                        QWithdrawRecord.withdrawRecord.arrivedAmount.as("arrivedAmount"),
                        QWithdrawRecord.withdrawRecord.status,
                        QWithdrawRecord.withdrawRecord.isAuto.as("isAuto"),
                        QWithdrawRecord.withdrawRecord.address,
                        QWithdrawRecord.withdrawRecord.createTime.as("createTime"),
                        QWithdrawRecord.withdrawRecord.fee,
                        QWithdrawRecord.withdrawRecord.transactionNumber.as("transactionNumber"),
                        QWithdrawRecord.withdrawRecord.remark,
                        QWithdrawRecord.withdrawRecord.errorMsg)
        ).from(QWithdrawRecord.withdrawRecord, QMember.member).where(predicates.toArray(new BooleanExpression[predicates.size()]));
        query.orderBy(orderSpecifiers.toArray(new OrderSpecifier[orderSpecifiers.size()]));
        List<WithdrawRecordVO> list = query.offset((pageModel.getPageNo() - 1) * pageModel.getPageSize()).limit(pageModel.getPageSize()).fetch();
        long total = query.fetchCount();
        return new PageImpl<>(list, pageModel.getPageable(), total);

    }

    public long countAuditing(){
        return withdrawApplyDao.countAllByStatus(WithdrawStatus.PROCESSING);
    }

    public List<WithdrawRecord> findByIds(Long[] ids){
        return withdrawApplyDao.findByIds(ids);
    }

    public Double countWithdrawAmountByTimeAndMemberIdAndCoin(Date startTime,Date endTime,Coin coin){
        return withdrawApplyDao.countWithdrawAmountByTimeAndMemberIdAndCoin(startTime,endTime,coin);
    }

    public Page<WithdrawRecord> findAll(Predicate predicate,PageModel pageModel){
        return withdrawApplyDao.findAll(predicate, pageModel.getPageable());
    }
}
