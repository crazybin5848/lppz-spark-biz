package com.lppz.spark.oms.utils;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lppz.oms.api.enums.LogisticsState;
import com.lppz.oms.api.enums.OrderState;
import com.lppz.oms.api.enums.OrderType;
import com.lppz.oms.api.enums.PaymentStatus;
import com.lppz.oms.api.enums.ReturnOrderState;
import com.lppz.oms.api.enums.ReturnType;

public class BuildExcleFileUtil {

	private static final Logger logger = LoggerFactory.getLogger(BuildExcleFileUtil.class);
	
	private Map<String,Map<String,String>> cacheMap;
	
	public BuildExcleFileUtil() {
	}
	public BuildExcleFileUtil(Map<String,Map<String,String>> cacheMap) {
		this.cacheMap = cacheMap;
	}
	
	public Map<String, Map<String, String>> getCacheMap() {
		return cacheMap;
	}
	public void setCacheMap(Map<String, Map<String, String>> cacheMap) {
		this.cacheMap = cacheMap;
	}
	public HSSFWorkbook createCancelWb(String[] header, List<Map<String,String>> datalist) {
		HSSFWorkbook hssWb = new HSSFWorkbook();
		final HSSFSheet sheet = hssWb.createSheet("取消单");
		HSSFRow row = sheet.createRow(0);
		final int cellNumber = header.length;

		for (int i = 0; i < cellNumber; i++) {
			ExcelUtil.createCell(row, i, header[i]);
			sheet.setColumnWidth(i, 20 * 256);
		}
		if (null != datalist && datalist.size() > 0) {
			int rowIdx = 1;
			for (Map<String,String> item : datalist) {
				row = sheet.createRow(rowIdx);

				if (StringUtils.isNotBlank(item.get("basestore"))) {
					ExcelUtil.createCell(row, 0,  item.get("basestore"));// 店铺
				}
				if (StringUtils.isNotBlank( item.get("outorderid"))) {
					ExcelUtil.createCell(row, 1,  item.get("outorderid"));// 外部订单号
				}

				if (StringUtils.isNotBlank( item.get("orderid"))) {
					ExcelUtil.createCell(row, 2,  item.get("orderid"));// 订单号
				}

				if (StringUtils.isNotBlank( item.get("returncode"))) {
					ExcelUtil.createCell(row, 3,  item.get("returncode"));// 取消单号
				}

				if (item.get("oramount_value") != null) {
					ExcelUtil.createCell(row, 4,
							(new BigDecimal(item.get("oramount_value"))).setScale(2, BigDecimal.ROUND_HALF_UP).toString());// 退款金额
				}

				if (StringUtils.isNotBlank( item.get("creemp"))) {
					ExcelUtil.createCell(row, 5,  item.get("creemp"));// 录入取消单客服姓名
				}

				if (item.get("creationtime") != null) {
					ExcelUtil.createCell(row, 6, item.get("creationtime"));// 录入取消单时间
				}
				rowIdx++;
			}
		}
		return hssWb;
	}

	public HSSFWorkbook createReturnWb(String[] header, List<Map<String,String>> datalist, Map<String, String> returnOnlyTypeMap) {
		HSSFWorkbook hssWb = new HSSFWorkbook();
		final HSSFSheet sheet = hssWb.createSheet("销退单");
		HSSFRow row = sheet.createRow(0);
		final int cellNumber = header.length;

		for (int i = 0; i < cellNumber; i++) {
			ExcelUtil.createCell(row, i, header[i]);
			sheet.setColumnWidth(i, 20 * 256);
		}
		if (null != datalist && datalist.size() > 0) {
			int rowIdx = 1;
			for (Map<String, String> item : datalist) {
				row = sheet.createRow(rowIdx);

				if (StringUtils.isNotBlank(item.get("basestore"))) {
					ExcelUtil.createCell(row, 0, item.get("basestore"));// 店铺
				}
				if (StringUtils.isNotBlank(item.get("outorderid"))) {
					ExcelUtil.createCell(row, 1, item.get("outorderid"));// 外部订单号
				}

				if (StringUtils.isNotBlank( item.get("orderid"))) {
					ExcelUtil.createCell(row, 2, item.get("orderid"));// 订单号
				}

				if (StringUtils.isNotBlank(item.get("returncode"))) {
					ExcelUtil.createCell(row, 3, item.get("returncode"));// 取消单号
				}

				if (StringUtils.isNotBlank(item.get("returntype"))) {
					ExcelUtil.createCell(row, 4, ReturnType.valueOf(item.get("returntype")).getName());// 退款类型
				}

				if (item.get("oramount_value") != null) {
					ExcelUtil.createCell(row, 5, (getBigDecimal(item.get("oramount_value"))).toString());// 退款金额
				}

				if (StringUtils.isNotBlank(item.get("creemp"))) {
					ExcelUtil.createCell(row, 6, item.get("creemp"));// 
				}

				if (item.get("creationtime") != null) {
					ExcelUtil.createCell(row, 7, item.get("creationtime"));// 录入取消单时间
				}

				if (StringUtils.isNotBlank(item.get("quantityvalue"))) {
					ExcelUtil.createCell(row, 8, item.get("quantityvalue"));// 退货退款项(数量)
				}

				if (StringUtils.isNotBlank(returnOnlyTypeMap.get(item.get("returnhbid")))) {
					ExcelUtil.createCell(row, 9, returnOnlyTypeMap.get(String.valueOf(item.get("returnhbid"))));// 退货类型
				}
				// 仅退款总金额
				BigDecimal refundonlyAmt = new BigDecimal("0.0").setScale(2, BigDecimal.ROUND_HALF_UP);
				if (refundonlyAmt.compareTo(BigDecimal.valueOf(0.0)) != 0) {
					ExcelUtil.createCell(row, 10, refundonlyAmt.toString());
				}
				
				if (item.get("SUPPLIERREASON_V2RefundAmount") != null) {
					ExcelUtil.createCell(row, 11,
							item.get("SUPPLIERREASON_V2RefundAmount"));// 
					refundonlyAmt = refundonlyAmt.add(getBigDecimal(item.get("SUPPLIERREASON_V2RefundAmount")));
				}

				if (item.get("SUPPLIERREASON_V2FreightAmount") != null) {
					ExcelUtil.createCell(row, 12,
							item.get("SUPPLIERREASON_V2FreightAmount"));// 
					refundonlyAmt = refundonlyAmt.add(getBigDecimal(item.get("SUPPLIERREASON_V2FreightAmount")));
				}

				if (StringUtils.isNotBlank( item.get("SUPPLIERREASON_V2note"))) {
					ExcelUtil.createCell(row, 13,  item.get("SUPPLIERREASON_V2note"));// 
				}

				if (item.get("WAREHOUSEREASON_V2RefundAmount") != null) {
					ExcelUtil.createCell(row, 14,
							item.get("WAREHOUSEREASON_V2RefundAmount"));// 
					refundonlyAmt = refundonlyAmt.add(getBigDecimal(item.get("WAREHOUSEREASON_V2RefundAmount")));
				}

				if (item.get("WAREHOUSEREASON_V2FreightAmount") != null) {
					ExcelUtil.createCell(row, 15,
							item.get("WAREHOUSEREASON_V2FreightAmount"));// 
					refundonlyAmt = refundonlyAmt.add(getBigDecimal(item.get("WAREHOUSEREASON_V2FreightAmount")));
				}

				if (StringUtils.isNotBlank( item.get("WAREHOUSEREASON_V2note"))) {
					ExcelUtil.createCell(row, 16,  item.get("WAREHOUSEREASON_V2note"));// 
				}

				if (item.get("EXPRESSREASON_V2RefundAmount") != null) {
					ExcelUtil.createCell(row, 17, item.get("EXPRESSREASON_V2RefundAmount"));// 
					refundonlyAmt = refundonlyAmt.add(getBigDecimal(item.get("EXPRESSREASON_V2RefundAmount")));
				}

				if (item.get("EXPRESSREASON_V2FreightAmount") != null) {
					ExcelUtil.createCell(row, 18,
							item.get("EXPRESSREASON_V2FreightAmount"));// 
					refundonlyAmt = refundonlyAmt.add(getBigDecimal(item.get("EXPRESSREASON_V2FreightAmount")));
				}

				if (StringUtils.isNotBlank( item.get("EXPRESSREASON_V2note"))) {
					ExcelUtil.createCell(row, 19,  item.get("EXPRESSREASON_V2note"));// 
				}

				if (item.get("STOREREASON_V2RefundAmount") != null) {
					ExcelUtil.createCell(row, 20, item.get("STOREREASON_V2RefundAmount"));// 
					refundonlyAmt = refundonlyAmt.add(getBigDecimal(item.get("STOREREASON_V2RefundAmount")));
				}

				if (item.get("STOREREASON_V2FreightAmount") != null) {
					ExcelUtil.createCell(row, 21,
							item.get("STOREREASON_V2FreightAmount"));// 
					refundonlyAmt = refundonlyAmt.add(getBigDecimal(item.get("STOREREASON_V2FreightAmount")));
				}

				if (StringUtils.isNotBlank( item.get("STOREREASON_V2note"))) {
					ExcelUtil.createCell(row, 22,  item.get("STOREREASON_V2note"));//
				}

				/*
				 * if (isBlank(item.get( "BUYEREASON_V2RefundAmount")))
				 * { ExcelUtil.createCell(row, 21,
				 * item.get("BUYEREASON_V2RefundAmount"));// 录入取消单时间 }
				 * 
				 * if (isBlank(item.get( "BUYEREASON_V2FreightAmount")))
				 * { ExcelUtil.createCell(row, 22,
				 * item.get("BUYEREASON_V2FreightAmount"));// 录入取消单时间 }
				 * 
				 * if (isBlank(item.get("BUYEREASON_V2note") )) {
				 * ExcelUtil.createCell(row, 23,
				 * item.get("BUYEREASON_V2note"));// 录入取消单时间 }
				 */

				if (StringUtils.isNotBlank( item.get("returnlocation"))) {
					ExcelUtil.createCell(row, 23,  item.get("returnlocation"));// 
				}

				if (StringUtils.isNotBlank( item.get("onlyreturnbackreceiver"))) {
					ExcelUtil.createCell(row, 24,  item.get("onlyreturnbackreceiver"));// 
				}

				if (StringUtils.isNotBlank( item.get("onlyreturnbackpayaccount"))) {
					ExcelUtil.createCell(row, 25,  item.get("onlyreturnbackpayaccount"));//
				}

				if (StringUtils.isNotBlank( item.get("state"))) {
					ExcelUtil.createCell(row, 26,  ReturnOrderState.valueOf(item.get("state")).getName());//  
				}

				if (StringUtils.isNotBlank( item.get("trackingid"))) {
					ExcelUtil.createCell(row, 27,  item.get("trackingid"));// 
				}
				rowIdx++;
			}
		}
		return hssWb;
	}
	
	private BigDecimal getBigDecimal(String number){
		return new BigDecimal(number)
		.setScale(2, BigDecimal.ROUND_HALF_UP);
	}

	public HSSFWorkbook createOrderStaticWb(final String[] orderHeader,
			final String[] orderLineHeader, final List<Map<String,String>> orderList,
			final List<Map<String,String>> orderLineList, final OrderType orderType) {
		// ==============创建订单头开始===========
		HSSFWorkbook hssWb = new HSSFWorkbook();
		final HSSFSheet sheet = hssWb.createSheet("订单头");
		HSSFRow row = sheet.createRow(0);
		final int cellNumber = orderHeader.length;

		for (int i = 0; i < cellNumber; i++) {
			ExcelUtil.createCell(row, i, orderHeader[i]);
			sheet.setColumnWidth(i, 20 * 256);
		}

		String orderId = null;
		String storeId = null;
		String stockroomlocationid = null;
		if (null != orderList && orderList.size() > 0) {
			int rowIdx = 1;
			for (final Map<String,String> item : orderList) {
				row = sheet.createRow(rowIdx);
				orderId = item.get("orderid");
				storeId = item.get("basestore");
				stockroomlocationid = item.get("stockroomlocationid");
				ExcelUtil.createCell(row, 0, item.get("outorderid"));// 外部订单号
				ExcelUtil.createCell(row, 1, orderId);// 订单号
				
//				BaseStorePo store = restClient.getBaseStoreById(storeId);
				String storeName = cacheMap.get(CacheUtil.KEY_BASESTORE).get(storeId);
				if (storeName != null) {
					ExcelUtil.createCell(row, 2, storeName);// 店铺名称 
//					if (store != null) {
//						ExcelUtil.createCell(row, 2, store.getName());// 店铺名称 
				}else{
					ExcelUtil.createCell(row, 2, storeId);// 店铺名称 
				}

//				StockroomLocationPo location = restClient.getStockRoomLocationById(stockroomlocationid);
				String locationName = cacheMap.get(CacheUtil.KEY_LOCATION).get(stockroomlocationid);
				if (locationName != null) {
					ExcelUtil.createCell(row, 3, locationName);// 仓库名称 
				}else{
					ExcelUtil.createCell(row, 3, stockroomlocationid);// 仓库名称 
				}

				String orderState = item.get("state");
				if (null != orderState) {
					orderState = OrderState.valueOf(orderState).getName();
				}
				ExcelUtil.createCell(row, 4, orderState);// 订单主状态

				String paymentStatus = item.get("paymentstatus");
				if (null != paymentStatus) {
					paymentStatus = PaymentStatus.valueOf(paymentStatus).getName();
				}
				ExcelUtil.createCell(row, 5, paymentStatus);// 支付子状态

				String logisticStatus = item.get("logisticstatus");
				if (null != logisticStatus) {
					logisticStatus = LogisticsState.valueOf(logisticStatus).getName();
				}
				ExcelUtil.createCell(row, 6, logisticStatus);// 物流子状态
				ExcelUtil.createCell(row, 7, isBlank(item.get("mergenumber")) 
						|| item.get("mergenumber").equalsIgnoreCase("NULL") ? "" : item.get("mergenumber"));// 合单号
				ExcelUtil.createCell(row, 8, item.get("issuedate"));// 下单时间
				ExcelUtil.createCell(row, 9, item.get("paymentdate"));// 支付时间
				ExcelUtil.createCell(row, 10, item.get("actualdeliverydate"));// 发货时间
				ExcelUtil.createCell(row, 11, item.get("paidamount"));// 实收金额
				ExcelUtil.createCell(row, 12, item.get("carriagefee"));// 运费
				ExcelUtil.createCell(row, 13, isBlank(item.get("username")) ? "" : item.get("username"));// 买家id
				ExcelUtil.createCell(row, 14, isBlank(item.get("shippingfirstname")) ? "" : item.get("shippingfirstname"));// 收货人
				ExcelUtil.createCell(row, 15, isBlank(item.get("shad_mobilephone")) ? "" : item.get("shad_mobilephone"));// 手机号码
				ExcelUtil.createCell(row, 16, isBlank(item.get("shad_phonenumber")) ? "" : item.get("shad_phonenumber"));// 电话

				String provinceName = cacheMap.get(CacheUtil.KEY_AREA).get(item.get("shad_countrysubentity"));
				if (provinceName != null) {
					ExcelUtil.createCell(row, 17, provinceName);// 省
				}else{
					ExcelUtil.createCell(row, 17, item.get("shad_countrysubentity"));// 省
				}
				
				String cityName = cacheMap.get(CacheUtil.KEY_AREA).get(item.get("shad_cityname"));
				if (cityName != null) {					
					ExcelUtil.createCell(row, 18,	cityName);// 市
				}else{
					ExcelUtil.createCell(row, 18,	item.get("shad_cityname"));// 市
				}
//				AreaPo disctrict = restClient.getAreaByCode(item.get("shad_name"));
				String disctrict = cacheMap.get(CacheUtil.KEY_AREA).get(item.get("shad_name"));
				if (disctrict != null) {					
					ExcelUtil.createCell(row, 19,	disctrict);// 区
				}else{
					ExcelUtil.createCell(row, 19,	item.get("shad_name"));// 区					
				}

				ExcelUtil.createCell(row, 20,
						isBlank(item.get("shad_addressline2")) ? "" : item.get("shad_addressline2"));// 地址
				ExcelUtil.createCell(row, 21, isBlank(item.get("logisticscode")) ? "" : item.get("logisticscode"));// 快递公司编码
				ExcelUtil.createCell(row, 22, isBlank(item.get("trackingid")) ? "" : item.get("trackingid"));// 运单号
				
				ExcelUtil.createCell(row, 23, isBlank(item.get("sendtime")) ? "" : item.get("sendtime"));// 入库时间

				String splitTag = "否";
				if ("1".equals(item.get("splittag"))) {
					splitTag = "是";
					ExcelUtil.createCell(row, 24, splitTag);// 是否拆单
					ExcelUtil.createCell(row, 25,
							item.get("createtime"));// 拆单时间
				} else {
					ExcelUtil.createCell(row, 24, splitTag);// 是否拆单
					ExcelUtil.createCell(row, 25, "");// 拆单时间为空
				}
				ExcelUtil.createCell(row, 26, isBlank(item.get("sellermemo")) ? "" : item.get("sellermemo"));// 备注信息
				ExcelUtil.createCell(row, 27, isBlank(item.get("emailid")) ? "" : item.get("emailid"));// 有赞对账
				ExcelUtil.createCell(row, 28, isBlank(item.get("source")) ? "" : item.get("source"));// 来源信息
				if (orderType !=null && StringUtils.equals("replenishOrder", orderType.getCode())) {
					ExcelUtil.createCell(row, 29, ReturnResonUtils.getReason(item.get("reissuetype")));// 补发类型
				}
				rowIdx++;
			}
		}
		// ==============创建订单头结束===========

		// ==============创建订单明细开始===========
		if (null != orderLineList && orderLineList.size() > 0) {
			final HSSFSheet orderLinesheet = hssWb.createSheet("订单明细");
			HSSFRow orderLinerow = orderLinesheet.createRow(0);
			final int orderLinecellNumber = orderLineHeader.length;

			for (int i = 0; i < orderLinecellNumber; i++) {
				ExcelUtil.createCell(orderLinerow, i, orderLineHeader[i]);
				orderLinesheet.setColumnWidth(i, 20 * 256);
			}
			// ============
			int rowIdx = 1;
			for (final Map<String,String> item : orderLineList) {
				orderLinerow = orderLinesheet.createRow(rowIdx);

				ExcelUtil.createCell(orderLinerow, 0, item.get("outorderid"));//
				// 外部订单号
				ExcelUtil.createCell(orderLinerow, 1, item.get("orderid"));// 订单号
				ExcelUtil.createCell(orderLinerow, 2, item.get("producttype"));//
				// 商品类型
				String skuid = item.get("skuid");
				ExcelUtil.createCell(orderLinerow, 3, skuid);// 商品编码
//				ProductPo product = restClient.getProductByProductId(skuid);
				String pruductName = cacheMap.get(CacheUtil.KEY_PRODUCT).get(skuid);
				if (pruductName != null) {
					ExcelUtil.createCell(orderLinerow, 4, pruductName);// 商品名称
				}
				ExcelUtil.createCell(orderLinerow, 5, item.get("quantityvalue"));// 商品数量
				ExcelUtil.createCell(orderLinerow, 6, item.get("paymentamount"));// 实付小计
				ExcelUtil.createCell(orderLinerow, 7,
						isBlank(item.get("bundleproductcode")) ? "" : item.get("bundleproductcode"));// 套装编码
				rowIdx++;
			}
		}
		// ==============创建订单明细结束===========
		return hssWb;
	}

	private boolean isBlank(String str) {
		if (str == null)
			return true;
		if (str.equalsIgnoreCase("null"))
			return true;
		if (str.equals(""))
			return true;
		if (str.equals(" "))
			return true;
		return false;
	}
}
