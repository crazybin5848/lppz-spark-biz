package com.lppz.spark.oms.utils;

import org.apache.commons.lang.StringUtils;

import com.lppz.oms.api.enums.OrderCategoryType;
import com.lppz.oms.api.model.HistOrderQueryParams;

public class SqlUtil {
	public static String buildCancelOrder_SQL(HistOrderQueryParams params) {
		StringBuffer sbf = new StringBuffer();
		sbf.append("SELECT a.id,");
		sbf.append("a.returncode,");
		sbf.append("cast(a.returnid as string),");
		sbf.append("a.state,");
		sbf.append("a.csremark,");
		sbf.append("a.creemp,");
		sbf.append("cast(a.oramount_value as string),");
		sbf.append("cast(a.creationtime as string),");
		sbf.append("a.type,");
		sbf.append("b.orderid,");
		sbf.append("b.outorderid,");
		sbf.append("b.mergenumber,");
		sbf.append("b.username,");
		sbf.append("b.shippingfirstname,");
		sbf.append("b.shad_mobilephone,");
		sbf.append("b.contact_telephone,");
		sbf.append(" b.basestore AS basestore ");
		sbf.append(" from omsextreturns a ");
		sbf.append(" LEFT JOIN omsextorders b ON a.`order` = b.id ");
		sbf.append(" WHERE a.type='cancelOrder' ");
//		if (StringUtils.isNotBlank(params.getStoreId())) {
//			sbf.append(" and b.basestore = '" + params.getStoreId() + "' ");
//		}
		if (StringUtils.isNotBlank(params.getReturnCodes())) {
			sbf.append(" and a.returnCode in ('" + params.getReturnCodes().replaceAll(",", "','") + "')");
		}
		if (StringUtils.isNotBlank(params.getOutorderIds())) {
			sbf.append(" and b.outOrderId in ('" + params.getOutorderIds().replaceAll(",", "','") + "')");
		}
		if (StringUtils.isNotBlank(params.getOrderIds())) {
			sbf.append(" and b.orderId in ('" + params.getOrderIds().replaceAll(",", "','") + "')");
		}
		if (StringUtils.isNotBlank(params.getReturnAmtStart())) {
			sbf.append(" and a.oramount_value  >= " + params.getReturnAmtStart() + " ");
		}
		if (StringUtils.isNotBlank(params.getReturnAmtEnd())) {
			sbf.append(" and a.oramount_value  <= " + params.getReturnAmtEnd() + " ");
		}
		if (StringUtils.isNotBlank(params.getUsername())) {
			sbf.append(" and b.username like '%" + params.getUsername() + "%' ");
		}
		if (StringUtils.isNotBlank(params.getReceiverName())) {
			sbf.append(" and b.shippingFirstName like '%" + params.getReceiverName()+ "%' ");
		}
		if (StringUtils.isNotBlank(params.getMobilephone())) {
			sbf.append(" and b.contact_telephone = '" + params.getMobilephone() + "' ");
		}
		if (StringUtils.isNotBlank(params.getTelephone())) {
			sbf.append(" and b.CONTACT_TELEFAX = '" + params.getTelephone() + "' ");
		}
		if ("send_wms".equalsIgnoreCase(params.getState())) {
			sbf.append(" AND a.state = 'draft' and b.state ='SENT_TO_WMS' ");
		}
		if ("draft".equalsIgnoreCase(params.getState())) {
			sbf.append(" AND a.state = 'draft' and b.state != 'SENT_TO_WMS' ");
		}
		if (!"send_wms".equalsIgnoreCase(params.getState())
				&& !"draft".equalsIgnoreCase(params.getState())&&StringUtils.isNotBlank(params.getState())) {
			sbf.append(" AND a.state = '" + params.getState() + "' ");
		}
		if (StringUtils.isNotBlank(params.getCreateDateStart())) {
			sbf.append(" and a.creationTime >= '" + params.getCreateDateStart() + "' ");
		}
		if (StringUtils.isNotBlank(params.getCreateDateEnd())) {
			sbf.append(" and a.creationTime <= '" + params.getCreateDateEnd() + "' ");
		}
		sbf.append(" order by a.CREATIONTIME desc ");
		return sbf.toString();
	}
	
	public static String buildReturnOrderPaged_SQL(HistOrderQueryParams params) {
		StringBuffer sbf = new StringBuffer();
		sbf.append(" SELECT DISTINCT a.id AS returnhbid,");
		sbf.append(" a.returncode,");
		sbf.append(" a.duty,");
		sbf.append(" cast(a.returnid as string),");
		sbf.append(" a.state,");
		sbf.append(" a.csremark,");
		sbf.append(" a.returntype,");
		sbf.append(" b.id AS orderhbid,");
		sbf.append(" b.orderid,");
		sbf.append(" b.outorderid,");
		sbf.append(" a.storecode,");
		sbf.append(" a.creemp,");
		sbf.append(" cast(a.oramount_value as string),");
		sbf.append(" b.username,");
		sbf.append(" b.shad_mobilephone AS contact_telephone,");
		sbf.append(" b.basestore,");
		sbf.append(" cast(a.stockbacktime as string),");
		sbf.append(" b.contact_note AS  contact,");
		sbf.append(" b.shippingfirstname AS shippingfirstname,");
		sbf.append(" cast(a.creationtime as string),");
		sbf.append(" a.type,");
		sbf.append(" a.returnlocation,");
		sbf.append(" c.trackingid,");
		sbf.append(" d.refundonlytype");
		sbf.append(" FROM omsextreturns a");
		sbf.append(" LEFT JOIN omsextorders b ON a.`order` = b.id");
		sbf.append(" LEFT JOIN omsextbusireturnpackagedata c on a.id=c.myreturn");
		sbf.append(" LEFT JOIN omsextbusirefundonlydata d on a.id=d.myreturn AND (d.refundamount != 0 OR d.freightamount !=0)");
		sbf.append(" WHERE a.type='returnOrder' ");
		if (StringUtils.isNotBlank(params.getReturnLocation())) {
			sbf.append(" and a.returnLocation = '" + params.getReturnLocation() + "' ");
		}
//		if (StringUtils.isNotBlank(params.getStoreId())) {
//			sbf.append(" and b.basestore = '" + params.getStoreId()+ "' ");
//		}
		if (StringUtils.isNotBlank(params.getTrackingid())) {
			sbf.append(" and c.trackingId in ('" + params.getTrackingid().replaceAll(",", "','") + "')");
		}

		if (StringUtils.isNotBlank(params.getReturnCodes())) {
			sbf.append(" and a.returnCode in ('" + params.getReturnCodes().replaceAll(",", "','") + "')");
		}
		if (StringUtils.isNotBlank(params.getOutorderIds())) {
			sbf.append(" and b.outOrderId in ('" + params.getOutorderIds().replaceAll(",", "','")+ "')");
		}
		if (StringUtils.isNotBlank(params.getOrderIds())) {
			sbf.append(" and b.orderId in ('" + params.getOrderIds().replaceAll(",", "','") + "')");
		}
		if (StringUtils.isNotBlank(params.getReturnAmtStart())) {
			sbf.append(" and a.oramount_value  >= " + params.getReturnAmtStart() + " ");
		}
		if (StringUtils.isNotBlank(params.getReturnAmtEnd())) {
			sbf.append(" and a.oramount_value  <= " + params.getReturnAmtEnd() + " ");
		}
		if (StringUtils.isNotBlank(params.getUsername())) {
			sbf.append(" and b.username like '%" + params.getUsername() + "%' ");
		}
		if (StringUtils.isNotBlank(params.getReturnCsremark())) {
			sbf.append(" and a.csremark like '%" + params.getReturnCsremark() + "%' ");
		}
		if (StringUtils.isNotBlank(params.getReceiverName())) {
			sbf.append(" and b.shippingFirstName like '%" + params.getReceiverName() + "%' ");
		}
		if (StringUtils.isNotBlank(params.getMobilephone())) {
			sbf.append(" and b.contact_telephone = '" + params.getMobilephone() + "' ");
		}
		if (StringUtils.isNotBlank(params.getTelephone())) {
			sbf.append(" and b.CONTACT_TELEFAX = '" + params.getTelephone() + "' ");
		}
		if ("send_wms".equalsIgnoreCase(params.getState())) {
			sbf.append(" AND a.state = 'draft' and b.state ='SENT_TO_WMS' ");
		}
		if ("draft".equalsIgnoreCase(params.getState())) {
			sbf.append(" AND a.state = 'draft' and b.state != 'SENT_TO_WMS' ");
		}
		if ( !"send_wms".equalsIgnoreCase(params.getState())
				&& !"draft".equalsIgnoreCase(params.getState())&&StringUtils.isNotBlank(params.getState())) {
			sbf.append(" AND a.state = '" + params.getState() + "' ");
		}
		if (StringUtils.isNotBlank(params.getCreateDateStart())) {
			sbf.append(" and a.creationTime >= '" + params.getCreateDateStart() + "' ");
		}
		if (StringUtils.isNotBlank(params.getCreateDateEnd())) {
			sbf.append(" and a.creationTime <= '" + params.getCreateDateEnd() + "' ");
		}
		if (StringUtils.isNotBlank(params.getReturnBackPayAccount())) {
			sbf.append(" AND (a.returnBackPayAccount like '%" + params.getReturnBackPayAccount()
					+ "%' or a.onlyreturnBackPayAccount like '%" + params.getReturnBackPayAccount() + "%' ");
		}
		if (StringUtils.isNotBlank(params.getReturnBackReceiver())) {
			sbf.append(" AND (a.returnbackreceiver like '%" + params.getReturnBackReceiver()
					+ "%' or a.onlyreturnbackreceiver like '%" + params.getReturnBackReceiver() + "%' ");
		}
		
		if (params.getReturnType() != null) {
			sbf.append(" and a.returnType = '" + params.getReturnType().getCode() + "' ");
		}
		
		if (params.getOrderCategoryType() != null) {
			String orderCatefory = params.getOrderCategoryType().getId();
			if ("6".equals(orderCatefory) || "0".equals(orderCatefory)  ) {
				sbf.append(" and a.orderCategory = '" +orderCatefory + "' ");
			} else {
				sbf.append(" and a.orderCategory in ('1','2') ");
			}
		}else{
			sbf.append(" and a.orderCategory not in ('1','2') ");			
		}
		sbf.append(" ORDER BY a.CREATIONTIME DESC");
		return sbf.toString();
	}
	
	//订单统计sql拼接 
	public static String buildOrderStatistics_SQL(HistOrderQueryParams params) {
		StringBuffer sbf = new StringBuffer();
		String head = "SELECT DISTINCT " +
			" t1.id orderHybrisId," +
			" t1.source," +
			" t1.orderid," + 
			" t1.outorderid," +
			" t1.basestore, " +
			" t1.state," +
			" t1.logisticstatus," +
			" t1.paymentstatus as paymentstatus," +
			" cast(t1.createdstate as string)," +
			" cast( t1.creationtime as string)," +
			" t1.mergeNumber," +
//			" t1.paymentdate," +
			" cast(t1.paidamount as string)," +
			" cast(t1.issuedate as string) as issuedate,"+
			" cast(t1.paymentdate as string) as paymentdate," +
			" cast(t1.carriagefee as string)," +
			" t1.username," +
			" t1.shippingfirstname," +
			" t1.shad_phonenumber," +
			" t1.shad_mobilephone," +
			" t1.shad_countrysubentity," +
			" t1.shad_cityname," +
			" t1.shad_name," +
			" t1.shad_addressline2," + 
			" cast(t1.issysmerge as string)," + 
			" cast(t1.splittag as string)," +
			" t1.sellermemo," +
			" t1.emailid," +
			" t1.reissuetype," +
			//" t8.storename as stockroomlocationid," +
			" t3.stockroomlocationid,"+
			" cast(t4.actualdeliverydate as string)," +
			" t4.logisticscode," +
			" t4.trackingid," + 
			" t2.producttype as producttype,"
			+ "t2.skuid skuid,"
			+ "t2.bundleproductid as bundleproductcode,"
			+ "cast(t2.unitpricevalue as string),"
			+ "cast(t2.quantityvalue as string) as quantityvalue,"
			+ "cast(t2.paymentamount as string) as paymentamount,"
			+ "t2.orderlineid as orderlineid"+
			" FROM omsextorders t1  "+
			" left join omsextorderlines t2 on t1.id = t2.myorder"+
			" left join omsextbusilpdeliveryedata t4 on t1.id = t4.myorder"+ 
		    " left join omsextshipments t3 on t1.id = t3.orderfk ";
		    //" left join omsextstockroomlocations t8 on t3.stockroomlocationid=t8.locationid"; 
		sbf.append(head);
		
		sbf.append(" where t1.STATE !='INVALID' AND t1.liangpintag !='1' ");
		
		//========开始拼接where条件=======
		if ( StringUtils.isNotBlank(params.getStockroomlocationid())) {
			sbf.append(" and t3.stockroomlocationid = '"+params.getStockroomlocationid()+"'" );
		}
		
		if (StringUtils.isNotBlank(params.getActualDeliveryDateStart()) ) {
			
			//actualdeliverydate 这个字段的时间值有问题，使用creationtime
			sbf.append(" AND t4.creationtime >='" + params.getActualDeliveryDateStart() + "' ");
		}
		if (StringUtils.isNotBlank(params.getActualDeliveryDateEnd()) ) {
			//actualdeliverydate 这个字段的时间值有问题，使用creationtime
			sbf.append(" AND t4.creationtime <='" + params.getActualDeliveryDateEnd() + "' ");
		}
		
		if (StringUtils.isNotBlank(params.getIsSysMerge()) ) {
			
			sbf.append(" AND t1.issysmerge =" + params.getIsSysMerge() );
		}
		
		if (StringUtils.isNotBlank(params.getIsSysMerge()) ) {
			
			sbf.append(" AND t1.splittag =" + params.getIsSysMerge());
		}
		
		OrderCategoryType orderCategory = params.getOrderCategoryType();
		if ( orderCategory != null ) {
			Integer  temp_orderCategory = Integer.valueOf(orderCategory.getId());
			if (temp_orderCategory == 0) {
				sbf.append(" AND t1.orderCategory in ('0','3','4','5','9') ");
			}else if(temp_orderCategory == 1) {
				sbf.append(" AND t1.orderCategory in ('1','2') ");
			}else{
				sbf.append(" AND t1.orderCategory in ('"+ temp_orderCategory +"') ");
			}
		}
		
		if (StringUtils.isNotBlank(params.getPaymentDateStart())) {
			sbf.append(" AND t1.paymentdate >='" + params.getPaymentDateStart() + "' ");
		}
		if (StringUtils.isNotBlank(params.getPaymentDateEnd())) {
			sbf.append(" AND t1.paymentdate <= '" + params.getPaymentDateEnd() + "' ");
		}
		
		if (StringUtils.isNotBlank(params.getOrderIds())) {
			sbf.append(" and t1.orderId in ('" + params.getOrderIds().replaceAll(",", "','") + "')");
		}
		if (StringUtils.isNotBlank(params.getOutorderIds())) {
			sbf.append(" and t1.outOrderId in ('" + params.getOutorderIds().replaceAll(",", "','") + "')");
		}
		if (StringUtils.isNotBlank(params.getUsername())) {
			sbf.append(" and t1.userName = '" + params.getUsername() + "' ");
		}
		
		if (StringUtils.isNotBlank(params.getPayAmoutFloor())) {
			sbf.append(" AND t1.PAIDAMOUNT <= '" + params.getPayAmoutFloor() + "' ");
		}
		if (StringUtils.isNotBlank(params.getPayAmoutRef())) {
			sbf.append(" AND t1.PAIDAMOUNT >= '" + params.getPayAmoutRef() + "' ");
		}
		
//		if (StringUtils.isNotBlank(params.getStoreId())) {
//			sbf.append(" AND t1.basestore = '" + params.getStoreId() + "' ");
//		}
		if (params.getOrderState() != null) {
			sbf.append(" AND t1.STATE = '" + params.getOrderState().getId() + "' ");
		}
		if (params.getOrderType() != null) {
			sbf.append(" and t1.orderType = '" + params.getOrderType().getCode() + "' ");
		}
		if (params.getLogisticState() == null) {
			sbf.append(" AND t1.LOGISTICSTATUS =  NULL ");
		}else{
			sbf.append(" AND t1.LOGISTICSTATUS =  '" + params.getLogisticState().getId() + "' ");
		}
		
		if (params.getPaymentStatus() != null) {
			sbf.append(" AND t1.paymentstatus = '" + params.getPaymentStatus() + "' ");
		}
		
		if (StringUtils.isNotBlank(params.getCreatedState())) {
			if (params.getCreatedState().equals("isnull")) {
				sbf.append(" AND t1.CREATEDSTATE = NULL ");
			} else if (params.getCreatedState().equals("isnotnull")) {
				sbf.append(" AND t1.CREATEDSTATE != NULL ");
			} else {
				sbf.append(" AND t1.CREATEDSTATE like '%" + params.getCreatedState() + "%'");
			}
		}
		
		

		sbf.append(" order by t1.paymentdate asc ");//注意：order by 字段必须为select中的字段，若表中字段和select中字段相同，使用t1.paymentdate不行，hive会报错
//		if (params.get("orderby") != null && StringUtils.isNotBlank((String) params.get("orderby"))
//				&& params.get("orderby").equals("asc")) {
//			sbf.append(" order by t1.paymentdate asc ");//注意：order by 字段必须为select中的字段，若表中字段和select中字段相同，使用t1.paymentdate不行，hive会报错
//		} else {
//			sbf.append(" order by t1.paymentdate desc ");
//		}
		return sbf.toString();
	}
}
