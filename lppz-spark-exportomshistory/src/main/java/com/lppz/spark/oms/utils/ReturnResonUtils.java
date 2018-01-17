package com.lppz.spark.oms.utils;

public class ReturnResonUtils {
	public static String getReason(String reasontype) {
		String restr = "";
		if ("STOREREASON_V2".equalsIgnoreCase(reasontype) || "store".equalsIgnoreCase(reasontype)) {
			restr = "店铺原因";
		} else if ("WAREHOUSEREASON_V2".equalsIgnoreCase(reasontype) || "none".equalsIgnoreCase(reasontype)) {
			restr = "仓库原因";
		} else if ("EXPRESSREASON_V2".equalsIgnoreCase(reasontype) || "broken".equalsIgnoreCase(reasontype)
				|| "miss".equalsIgnoreCase(reasontype)) {
			restr = "快递原因";
		} else if ("SUPPLIERREASON_V2".equalsIgnoreCase(reasontype) || "supplier".equalsIgnoreCase(reasontype)) {
			restr = "供应商原因";
		} else if ("supplier".equalsIgnoreCase(reasontype)) {
			restr = "买家原因";
		}

		// 订单统计：补发单原因
		// REORDER_TYPE.put("none", "仓库原因");
		// REORDER_TYPE.put("miss", "快递原因");
		// REORDER_TYPE.put("broken", "快递原因");
		// REORDER_TYPE.put("supplier", "供应商原因");
		// REORDER_TYPE.put("store", "店铺原因");
		// REORDER_TYPE.put("buyer", "买家原因");
		return restr;
	}
}
