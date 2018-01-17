package com.lppz.spark.oms.utils;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lppz.oms.api.enums.OrderCategoryType;
import com.lppz.oms.api.enums.OrderType;

public class ExportExcelFileUtil {
	private Logger logger = LoggerFactory.getLogger(ExportExcelFileUtil.class);
	//导出表格表头
	private String[] orderHeads = { "外部订单号", "oms订单号", "店铺名称", "仓库编码", "主状态",
			"支付子状态", "物流子状态", "合单号", "下单时间", "支付时间", "发货时间", "实收金额", "运费",
			"买家id", "收货人", "手机", "电话", "省", "市", "区", "地址", "快递公司编码",
			"运单号", "入库时间", "是否拆单", "拆单时间", "备注", "有赞对账","来源类型" };
	private String[] reorderHeaders = { "外部订单号", "oms订单号", "店铺名称", "仓库编码",
			"主状态", "支付子状态", "物流子状态", "合单号", "下单时间", "支付时间", "发货时间", "实收金额",
			"运费", "买家id", "收货人", "手机", "电话", "省", "市", "区", "地址", "快递公司编码",
			"运单号", "入库时间", "是否拆单", "拆单时间", "备注", "有赞对账", "来源类型","补发类型"  };

	private String[] orderDetailHeads = { "外部订单号", "oms订单号", "商品类型", "商家编码",
			"商品名称", "商品数量", "实付小计", "套装编码" };
	
	private String[] cancelHheader = { "店铺", "外部订单号", "oms订单号", "取消单号", "退款金额", "录入取消单的客服姓名", "录入取消单时间" };
	
	private String[] returnHeader = { "店铺", "外部订单号", "oms订单号", "销退单号", "销退类型",
			"退款金额", "录入销退单客服姓名", "录入销退单时间", "退货退款项","退货类型","仅退款总金额",
			"供应商原因退款金额","供应商原因退运费", "供应商原因备注",
			"仓库原因退款金额","仓库原因退运费", "仓库原因备注", 
			"快递原因退款金额","快递原因退运费", "快递原因备注", 
			"店铺原因退款金额","店铺原因退运费", "店铺原因备注", 
//			"买家原因退款金额","买家原因退运费", "买家原因备注", 
//			 "供应商原因", "退款金额","退运费", "备注",
			"退货仓库", "打款人姓名", "打款账号", "销退单状态", "运单号" };
	
	private BuildExcleFileUtil buildUtil;
	
	public ExportExcelFileUtil() {
	}
	
	public ExportExcelFileUtil(Map<String,Map<String,String>> cacheMap){
		this.buildUtil=new BuildExcleFileUtil(cacheMap);
	}
	
	public HSSFWorkbook exportOrderstaticExcel(List<Map<String,String>> orderList, List<Map<String,String>> orderlineList
			, String orderTypeStr, String orderCategoryStr){
		
		OrderType orderType =  StringUtils.isBlank(orderTypeStr) ? null:OrderType.valueOf(orderTypeStr);
		OrderCategoryType orderCategory = StringUtils.isBlank(orderCategoryStr) ? null:OrderCategoryType.valueOf(orderCategoryStr);
		String[] orderHeader = null; 
		if (null != orderList && orderList.size() > 0) {
			
			//非补发单和买手单，不需要导出订单行信息,新需求所有订单都需要导出订单行
			 if (!OrderType.REPLENISH_ORDER.equals(orderType) &&  !OrderCategoryType.MAI_SHOU.equals(
					orderCategory)) {
				 logger.debug("非补发单和买手单，开始组装导出数据到Excel中");
				 orderHeader = orderHeads;
				logger.debug("非补发单和买手单，完成组装导出数据到Excel中");
					
			} else {
				//买手
				if (OrderCategoryType.MAI_SHOU.equals(orderCategory)) {
					orderHeader = orderHeads;
					logger.debug("买手单，开始组装导出数据到Excel中");
					
				}else{
					//补发单，换了reorderHeaders
					logger.debug("补发单，开始组装导出数据到Excel中");
					orderHeader = reorderHeaders;
					logger.debug("补发单，完成组装导出数据到Excel中");
				}
			}
		} else {
			return null;
		}
		return buildUtil.createOrderStaticWb(orderHeader, orderDetailHeads, orderList, orderlineList,  orderType);
	}
	
	public HSSFWorkbook exportReturnExcel(List<Map<String,String>> datalist, Map<String,String> returnOnlyTypeMap){
		return buildUtil.createReturnWb(returnHeader, datalist, returnOnlyTypeMap);
	}
	
	public HSSFWorkbook exportCancelExcel(List<Map<String,String>> datalist){
			return buildUtil.createCancelWb(cancelHheader, datalist);
	}
}
