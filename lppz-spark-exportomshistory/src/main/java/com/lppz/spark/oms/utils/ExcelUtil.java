package com.lppz.spark.oms.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFDataFormat;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.util.Region;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFDataFormat;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.lppz.spark.scala.SparkHdfsUtil;

public class ExcelUtil {

	// 定制日期格式
		private static String DATE_FORMAT = "yyyy-MM-dd"; // "m/d/yy h:mm"
		// 定制浮点数格式
		private static String NUMBER_FORMAT = "#,##0.00";

		/**
		 * 导出Excel文件
		 *
		 * @throws IOException
		 */
		public static String exportExcel(final HSSFWorkbook wb, final String fileName,
				final String filePath)
				throws FileNotFoundException, IOException {
			final StringBuilder filePathSb = new StringBuilder(filePath);
			String name = fileName;
			if (StringUtils.isBlank(fileName)) {
				name = "noname";
			}

			filePathSb.append(System.getProperty("file.separator"))
					.append("exportExcel")
					.append(System.getProperty("file.separator"))
					.append("staticOrder");

			final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
			final String nameTimestamp = sdf.format(new Date());

			final File folder = new File(filePathSb.toString());

			if (!folder.exists()) {
				folder.mkdirs();
			}

			filePathSb.append(System.getProperty("file.separator"))
					.append("staticOrder_").append(name).append("_")
					.append(nameTimestamp).append(".xls");
			FileOutputStream fos = null;
			try {
	            fos = new FileOutputStream(new File(filePathSb.toString()));
	             wb.write(fos);
	            return filePathSb.toString();
	        } finally {
			    if (fos != null) {
	                fos.close();
	            }
	        }
		}
		
		public static void saveFileToHdfs(HSSFWorkbook wb,String hdfsFileStr,String hdfsUrl) throws IOException{
			Path hdfsFile = new Path(hdfsFileStr);
			FSDataOutputStream out = null;
			try {
				FileSystem hdfs = new SparkHdfsUtil().getFileSystem(hdfsUrl);
				out = hdfs.create(hdfsFile);
	             wb.write(out);
	        } finally {
			    if (out != null) {
			    	out.close();
	            }
	        }
		}

		/**
		 * 导出Excel下载文件
		 *
		 * @throws IOException
		 *
		 * @throws IOException
		 */

		public static void downloadExcel(final HttpServletResponse response,
				final Object wb, final String fileName) throws IOException {

			response.setContentType("text/html; charset=UTF-8");
			response.setHeader("Content-Disposition", "attachment; filename="
					+ ExcelUtil.getDownloadFileName(fileName));

			final OutputStream os = response.getOutputStream();

			if (wb instanceof HSSFWorkbook) {
				final HSSFWorkbook hssWb = (HSSFWorkbook) wb;
				response.setContentType("xls");
				hssWb.write(os);
			}
			if (wb instanceof org.apache.poi.xssf.usermodel.XSSFWorkbook) {
				final XSSFWorkbook xssWb = (XSSFWorkbook) wb;
				response.setContentType("xlsx");
				xssWb.write(os);
			}
			os.flush();
			os.close();
		}

		/**
		 * 设置单元格
		 *
		 * @param row
		 *            单元格所在的行
		 * @param colIndex
		 *            列号
		 * @param value
		 *            单元格填充整数值
		 * @return 新生成的单元格
		 */
		public static Object setCell(final Object row, final int colIndex,
				final int value) {
			Object cell = null;
			if (row instanceof HSSFRow) {
				final HSSFRow hssWb = (HSSFRow) row;
				cell = hssWb.getCell(colIndex);
				((HSSFCell) cell).setCellType(HSSFCell.CELL_TYPE_NUMERIC);
				((HSSFCell) cell).setCellValue(value);
			}
			if (row instanceof org.apache.poi.xssf.usermodel.XSSFRow) {
				final XSSFRow xssWb = (XSSFRow) row;
				cell = xssWb.getCell(colIndex);
				((XSSFCell) cell).setCellType(XSSFCell.CELL_TYPE_NUMERIC);
				((XSSFCell) cell).setCellValue(value);
			}
			// XSSFCell cell = row.getCell(colIndex);

			return cell;
		}

		/**
		 * 设置单元格
		 *
		 * @param row
		 *            单元格所在的行
		 * @param colIndex
		 *            列号
		 * @param value
		 *            单元格填充整数值
		 * @param cellStyle
		 *            单元格样式
		 * @return 新生成的单元格
		 */
		public static Object setCell(final Object row, final int colIndex,
				final int value, final Object cellStyle) {
			Object cell = null;

			if (row instanceof HSSFRow) {
				final HSSFRow objRow = (HSSFRow) row;
				cell = setCell(objRow, colIndex, value);
				final HSSFCellStyle objCellStyle = (HSSFCellStyle) cellStyle;
				((HSSFCell) cell).setCellStyle(objCellStyle);
			}
			if (row instanceof org.apache.poi.xssf.usermodel.XSSFRow) {
				final XSSFRow objRow = (XSSFRow) row;
				cell = setCell(objRow, colIndex, value);
				final XSSFCellStyle objCellStyle = (XSSFCellStyle) cellStyle;
				((XSSFCell) cell).setCellStyle(objCellStyle);
			}
			return cell;
		}

		/**
		 * 设置单元格
		 *
		 * @param row
		 *            单元格所在的行
		 * @param colIndex
		 *            列号
		 * @param value
		 *            单元格double值
		 * @return 新生成的单元格
		 */
		public static Object setCell(final Object row, final int colIndex,
				final double value) {
			Object cell = null;
			if (row instanceof HSSFRow) {
				final HSSFRow hssWb = (HSSFRow) row;
				cell = hssWb.getCell(colIndex);
				((HSSFCell) cell).setCellType(HSSFCell.CELL_TYPE_NUMERIC);
				((HSSFCell) cell).setCellValue(value);
			}
			if (row instanceof org.apache.poi.xssf.usermodel.XSSFRow) {
				final XSSFRow xssWb = (XSSFRow) row;
				cell = xssWb.getCell(colIndex);
				((XSSFCell) cell).setCellType(XSSFCell.CELL_TYPE_NUMERIC);
				((XSSFCell) cell).setCellValue(value);
			}
			// XSSFCell cell = row.getCell(colIndex);

			return cell;
		}

		/**
		 * 设置单元格
		 *
		 * @param row
		 *            单元格所在的行
		 * @param colIndex
		 *            列号
		 * @param value
		 *            单元格double值
		 * @param cellStyle
		 *            单元格样式
		 * @return 新生成的单元格
		 */
		public static Object setCell(final Object row, final int colIndex,
				final double value, final Object cellStyle) {
			Object cell = null;

			if (row instanceof HSSFRow) {
				final HSSFRow objRow = (HSSFRow) row;
				cell = setCell(objRow, colIndex, value);
				final HSSFCellStyle objCellStyle = (HSSFCellStyle) cellStyle;
				((HSSFCell) cell).setCellStyle(objCellStyle);
			}
			if (row instanceof org.apache.poi.xssf.usermodel.XSSFRow) {
				final XSSFRow objRow = (XSSFRow) row;
				cell = setCell(objRow, colIndex, value);
				final XSSFCellStyle objCellStyle = (XSSFCellStyle) cellStyle;
				((XSSFCell) cell).setCellStyle(objCellStyle);
			}
			return cell;
		}

		/**
		 * 设置单元格
		 *
		 * @param row
		 *            单元格所在的行
		 * @param colIndex
		 *            列号
		 * @param value
		 *            单元格填充字符串值
		 * @return 新生成的单元格
		 */
		public static Object setCell(final Object row, final int colIndex,
				final String value) {
			Object cell = null;
			if (row instanceof HSSFRow) {
				final HSSFRow hssWb = (HSSFRow) row;
				cell = hssWb.getCell(colIndex);
				((HSSFCell) cell).setCellType(HSSFCell.CELL_TYPE_STRING);
				((HSSFCell) cell).setCellType(HSSFCell.ENCODING_UTF_16);
				((HSSFCell) cell).setCellValue(value);
			}
			if (row instanceof org.apache.poi.xssf.usermodel.XSSFRow) {
				final XSSFRow xssWb = (XSSFRow) row;
				cell = xssWb.getCell(colIndex);
				((XSSFCell) cell).setCellType(XSSFCell.CELL_TYPE_STRING);
				((XSSFCell) cell).setCellValue(value);
			}
			return cell;
		}

		/**
		 * 设置单元格
		 *
		 * @param row
		 *            单元格所在的行
		 * @param colIndex
		 *            列号
		 * @param value
		 *            单元格填充字符串值
		 * @param cellStyle
		 *            单元格样式
		 * @return 新生成的单元格
		 */
		public static Object setCell(final Object row, final int colIndex,
				final String value, final Object cellStyle) {
			Object cell = null;

			if (row instanceof HSSFRow) {
				final HSSFRow objRow = (HSSFRow) row;
				cell = setCell(objRow, colIndex, value);
				final HSSFCellStyle objCellStyle = (HSSFCellStyle) cellStyle;
				((HSSFCell) cell).setCellStyle(objCellStyle);
			}
			if (row instanceof org.apache.poi.xssf.usermodel.XSSFRow) {
				final XSSFRow objRow = (XSSFRow) row;
				cell = setCell(objRow, colIndex, value);
				final XSSFCellStyle objCellStyle = (XSSFCellStyle) cellStyle;
				((XSSFCell) cell).setCellStyle(objCellStyle);
			}
			return cell;
		}

		/**
		 * 设置单元格
		 *
		 * @param row
		 *            单元格所在的行
		 * @param colIndex
		 *            列号
		 * @param value
		 *            单元格填充日期值
		 * @return 新生成的单元格
		 */
		public static Object setCell(final Object row, final int colIndex,
				final Date value) {
			Object cell = null;
			if (row instanceof HSSFRow) {
				final HSSFRow hssWb = (HSSFRow) row;
				cell = hssWb.getCell(colIndex);
				if (value == null) {
					((HSSFCell) cell).setCellValue("");
				} else {
					((HSSFCell) cell).setCellValue(value);
				}
			}
			if (row instanceof org.apache.poi.xssf.usermodel.XSSFRow) {
				final XSSFRow xssWb = (XSSFRow) row;
				cell = xssWb.getCell(colIndex);
				if (value == null) {
					((XSSFCell) cell).setCellValue("");
				} else {
					((XSSFCell) cell).setCellValue(value);
				}
			}
			return cell;
		}

		/**
		 * 设置单元格
		 *
		 * @param row
		 *            单元格所在的行
		 * @param colIndex
		 *            列号
		 * @param value
		 *            单元格填充日期值
		 * @param cellStyle
		 *            单元格样式
		 * @return 新生成的单元格
		 */
		public static Object setCell(final Object row, final int colIndex,
				final Date value, final Object cellStyle) {
			Object cell = null;

			if (row instanceof HSSFRow) {
				final HSSFRow objRow = (HSSFRow) row;
				cell = objRow.getCell(colIndex);
				final HSSFCellStyle objCellStyle = (HSSFCellStyle) cellStyle;
				((HSSFCell) cell).setCellStyle(objCellStyle);
				((HSSFCell) cell).setCellValue(value);
			}
			if (row instanceof org.apache.poi.xssf.usermodel.XSSFRow) {
				final XSSFRow objRow = (XSSFRow) row;
				cell = objRow.getCell(colIndex);
				final XSSFCellStyle objCellStyle = (XSSFCellStyle) cellStyle;
				((XSSFCell) cell).setCellStyle(objCellStyle);
				((XSSFCell) cell).setCellValue(value);
			}
			return cell;
		}

		/**
		 * 设置公式单元格
		 *
		 * @param row
		 *            单元格所在的行
		 * @param colIndex
		 *            列号
		 * @param formulaString
		 *            单元格填充公式
		 * @return 新生成的单元格
		 */
		public static Object setFormulaCell(final Object row, final int colIndex,
				final String formulaString) {
			Object cell = null;
			if (row instanceof HSSFRow) {
				final HSSFRow hssWb = (HSSFRow) row;
				cell = hssWb.getCell(colIndex);
				((HSSFCell) cell).setCellFormula(formulaString);
			}
			if (row instanceof org.apache.poi.xssf.usermodel.XSSFRow) {
				final XSSFRow xssWb = (XSSFRow) row;
				cell = xssWb.getCell(colIndex);
				((XSSFCell) cell).setCellFormula(formulaString);
			}
			return cell;

		}

		/**
		 * 设置公式单元格
		 *
		 * @param row
		 *            单元格所在的行
		 * @param colIndex
		 *            列号
		 * @param formulaString
		 *            单元格填充公式
		 * @param cellStyle
		 *            单元格样式
		 * @return 新生成的单元格
		 */
		public static Object setFormulaCell(final Object row, final int colIndex,
				final String formulaString, final Object cellStyle) {
			Object cell = null;

			if (row instanceof HSSFRow) {
				final HSSFRow objRow = (HSSFRow) row;
				cell = objRow.getCell(colIndex);
				((HSSFCell) cell).setCellFormula(formulaString);
				final HSSFCellStyle objCellStyle = (HSSFCellStyle) cellStyle;
				((HSSFCell) cell).setCellStyle(objCellStyle);
			}
			if (row instanceof org.apache.poi.xssf.usermodel.XSSFRow) {
				final XSSFRow objRow = (XSSFRow) row;
				cell = objRow.getCell(colIndex);
				((XSSFCell) cell).setCellFormula(formulaString);
				final XSSFCellStyle objCellStyle = (XSSFCellStyle) cellStyle;
				((XSSFCell) cell).setCellStyle(objCellStyle);
			}
			return cell;
		}

		/**
		 * 创建单元格
		 *
		 * @param row
		 *            单元格所在的行
		 * @param colIndex
		 *            列号
		 * @param value
		 *            单元格填充整数值
		 * @return 新生成的单元格
		 */
		public static Object createCell(final Object row, final int colIndex,
				final int value) {
			Object cell = null;
			if (row instanceof HSSFRow) {
				final HSSFRow hssWb = (HSSFRow) row;
				cell = hssWb.createCell(colIndex);
				((HSSFCell) cell).setCellType(HSSFCell.CELL_TYPE_NUMERIC);
				((HSSFCell) cell).setCellValue(value);
			}
			if (row instanceof org.apache.poi.xssf.usermodel.XSSFRow) {
				final XSSFRow xssWb = (XSSFRow) row;
				cell = xssWb.createCell(colIndex);
				((XSSFCell) cell).setCellType(XSSFCell.CELL_TYPE_NUMERIC);
				((XSSFCell) cell).setCellValue(value);
			}
			return cell;

		}

		/**
		 * 创建单元格
		 *
		 * @param row
		 *            单元格所在的行
		 * @param colIndex
		 *            列号
		 * @param value
		 *            单元格填充整数值
		 * @param cellStyle
		 *            单元格样式
		 * @return 新生成的单元格
		 */
		public static Object createCell(final Object row, final int colIndex,
				final int value, final Object cellStyle) {
			Object cell = null;

			if (row instanceof HSSFRow) {
				final HSSFRow objRow = (HSSFRow) row;
				cell = createCell(objRow, colIndex, value);
				final HSSFCellStyle objCellStyle = (HSSFCellStyle) cellStyle;
				((HSSFCell) cell).setCellStyle(objCellStyle);
			}
			if (row instanceof org.apache.poi.xssf.usermodel.XSSFRow) {
				final XSSFRow objRow = (XSSFRow) row;
				cell = createCell(objRow, colIndex, value);
				final XSSFCellStyle objCellStyle = (XSSFCellStyle) cellStyle;
				((XSSFCell) cell).setCellStyle(objCellStyle);
			}
			return cell;
		}

		/**
		 * 创建单元格
		 *
		 * @param row
		 *            单元格所在的行
		 * @param colIndex
		 *            列号
		 * @param value
		 *            单元格double值
		 * @return 新生成的单元格
		 */
		public static Object createCell(final Object row, final int colIndex,
				final double value) {
			Object cell = null;
			if (row instanceof HSSFRow) {
				final HSSFRow hssWb = (HSSFRow) row;
				cell = hssWb.createCell(colIndex);
				((HSSFCell) cell).setCellType(HSSFCell.CELL_TYPE_NUMERIC);
				((HSSFCell) cell).setCellValue(value);
			}
			if (row instanceof org.apache.poi.xssf.usermodel.XSSFRow) {
				final XSSFRow xssWb = (XSSFRow) row;
				cell = xssWb.createCell(colIndex);
				((XSSFCell) cell).setCellType(XSSFCell.CELL_TYPE_NUMERIC);
				((XSSFCell) cell).setCellValue(value);
			}
			return cell;
		}

		/**
		 * 创建单元格
		 *
		 * @param row
		 *            单元格所在的行
		 * @param colIndex
		 *            列号
		 * @param value
		 *            单元格double值
		 * @param cellStyle
		 *            单元格样式
		 * @return 新生成的单元格
		 */
		public static Object createCell(final Object row, final int colIndex,
				final double value, final Object cellStyle) {
			Object cell = null;

			if (row instanceof HSSFRow) {
				final HSSFRow objRow = (HSSFRow) row;
				cell = createCell(objRow, colIndex, value);
				final HSSFCellStyle objCellStyle = (HSSFCellStyle) cellStyle;
				((HSSFCell) cell).setCellStyle(objCellStyle);
			}
			if (row instanceof org.apache.poi.xssf.usermodel.XSSFRow) {
				final XSSFRow objRow = (XSSFRow) row;
				cell = createCell(objRow, colIndex, value);
				final XSSFCellStyle objCellStyle = (XSSFCellStyle) cellStyle;
				((XSSFCell) cell).setCellStyle(objCellStyle);
			}
			return cell;

		}

		/**
		 * 创建单元格
		 *
		 * @param row
		 *            单元格所在的行
		 * @param colIndex
		 *            列号
		 * @param value
		 *            单元格填充字符串值
		 * @return 新生成的单元格
		 */
		public static Object createCell(final Object row, final int colIndex,
				final String value) {
			Object cell = null;
			if (row instanceof HSSFRow) {
				final HSSFRow hssWb = (HSSFRow) row;
				cell = hssWb.createCell(colIndex);
				((HSSFCell) cell).setCellType(HSSFCell.CELL_TYPE_STRING);
				((HSSFCell) cell).setCellType(HSSFCell.ENCODING_UTF_16);
				((HSSFCell) cell).setCellValue(value);
			}
			if (row instanceof org.apache.poi.xssf.usermodel.XSSFRow) {
				final XSSFRow xssWb = (XSSFRow) row;
				cell = xssWb.createCell(colIndex);
				((XSSFCell) cell).setCellType(XSSFCell.CELL_TYPE_STRING);
				((XSSFCell) cell).setCellValue(value);
			}
			return cell;
		}

		/**
		 * 创建单元格
		 *
		 * @param row
		 *            单元格所在的行
		 * @param colIndex
		 *            列号
		 * @param value
		 *            单元格填充字符串值
		 * @param cellStyle
		 *            单元格样式
		 * @return 新生成的单元格
		 */
		public static Object createCell(final Object row, final int colIndex,
				final String value, final Object cellStyle) {
			Object cell = null;

			if (row instanceof HSSFRow) {
				final HSSFRow objRow = (HSSFRow) row;
				cell = createCell(objRow, colIndex, value);
				final HSSFCellStyle objCellStyle = (HSSFCellStyle) cellStyle;
				((HSSFCell) cell).setCellStyle(objCellStyle);
			}
			if (row instanceof org.apache.poi.xssf.usermodel.XSSFRow) {
				final XSSFRow objRow = (XSSFRow) row;
				cell = createCell(objRow, colIndex, value);
				final XSSFCellStyle objCellStyle = (XSSFCellStyle) cellStyle;
				((XSSFCell) cell).setCellStyle(objCellStyle);
			}
			return cell;
		}

		/**
		 * 创建单元格
		 *
		 * @param row
		 *            单元格所在的行
		 * @param colIndex
		 *            列号
		 * @param value
		 *            单元格填充日期值
		 * @return 新生成的单元格
		 */
		public static Object createCell(final Object wb, final Object row,
				final int colIndex, final Date value) {
			Object cell = null;
			if (wb instanceof HSSFWorkbook) {
				final HSSFRow hssWb = (HSSFRow) row;
				final HSSFDataFormat format = ((HSSFWorkbook) wb)
						.createDataFormat();
				cell = hssWb.createCell(colIndex);
				final HSSFCellStyle style = ((HSSFWorkbook) wb).createCellStyle();
				style.setDataFormat(format.getFormat(DATE_FORMAT));

				((HSSFCell) cell).setCellValue(value);
				((HSSFCell) cell).setCellStyle(style);
			}
			if (wb instanceof org.apache.poi.xssf.usermodel.XSSFWorkbook) {
				final XSSFRow xssWb = (XSSFRow) row;
				final XSSFDataFormat format = ((XSSFWorkbook) wb)
						.createDataFormat();
				cell = xssWb.createCell(colIndex);
				final XSSFCellStyle style = ((XSSFWorkbook) wb).createCellStyle();
				style.setDataFormat(format.getFormat(DATE_FORMAT));

				((XSSFCell) cell).setCellValue(value);
				((XSSFCell) cell).setCellStyle(style);
			}
			return cell;
		}

		/**
		 * 创建单元格
		 *
		 * @param row
		 *            单元格所在的行
		 * @param colIndex
		 *            列号
		 * @param value
		 *            单元格填充日期值
		 * @return 新生成的单元格
		 */
		public static Object createCell(final Object wb, final Object row,
				final int colIndex, final Date value, final Object cellStyle) {
			Object cell = null;
			if (wb instanceof HSSFWorkbook) {
				final HSSFDataFormat format = ((HSSFWorkbook) wb)
						.createDataFormat();
				cell = ((HSSFRow) row).createCell(colIndex);
				final HSSFCellStyle style = ((HSSFWorkbook) wb).createCellStyle();
				style.cloneStyleFrom((HSSFCellStyle) cellStyle);
				style.setDataFormat(format.getFormat(DATE_FORMAT));

				((HSSFCell) cell).setCellValue(value);
				((HSSFCell) cell).setCellStyle(style);
			}
			if (wb instanceof org.apache.poi.xssf.usermodel.XSSFWorkbook) {
				final XSSFDataFormat format = ((XSSFWorkbook) wb)
						.createDataFormat();
				cell = ((XSSFRow) row).createCell(colIndex);
				final XSSFCellStyle style = ((XSSFWorkbook) wb).createCellStyle();
				style.cloneStyleFrom((XSSFCellStyle) cellStyle);
				style.setDataFormat(format.getFormat(DATE_FORMAT));

				((XSSFCell) cell).setCellValue(value);
				((XSSFCell) cell).setCellStyle(style);
			}
			return cell;
		}

		/**
		 * 创建单元格
		 *
		 * @param row
		 *            单元格所在的行
		 * @param colIndex
		 *            列号
		 * @param value
		 *            单元格填充日期值
		 * @return 新生成的单元格
		 */
		public static Object createCell(final Object wb, final Object row,
				final int colIndex, final Date value, final String dateFormat) {
			Object cell = null;
			if (wb instanceof HSSFWorkbook) {
				final HSSFRow hssWb = (HSSFRow) row;
				final HSSFDataFormat format = ((HSSFWorkbook) wb)
						.createDataFormat();
				cell = hssWb.createCell(colIndex);
				final HSSFCellStyle style = ((HSSFWorkbook) wb).createCellStyle();
				style.setDataFormat(format.getFormat(dateFormat));

				((HSSFCell) cell).setCellValue(value);
				((HSSFCell) cell).setCellStyle(style);
			}
			if (wb instanceof org.apache.poi.xssf.usermodel.XSSFWorkbook) {
				final XSSFRow xssWb = (XSSFRow) row;
				final XSSFDataFormat format = ((XSSFWorkbook) wb)
						.createDataFormat();
				cell = xssWb.createCell(colIndex);
				final XSSFCellStyle style = ((XSSFWorkbook) wb).createCellStyle();
				style.setDataFormat(format.getFormat(dateFormat));

				((XSSFCell) cell).setCellValue(value);
				((XSSFCell) cell).setCellStyle(style);
			}
			return cell;
		}

		/**
		 * 创建单元格
		 *
		 * @param row
		 *            单元格所在的行
		 * @param colIndex
		 *            列号
		 * @param value
		 *            单元格填充日期值
		 * @return 新生成的单元格
		 */
		public static Object createCell(final Object wb, final Object row,
				final int colIndex, final Date value, final Object cellStyle,
				final String dateFormat) {
			Object cell = null;
			if (wb instanceof HSSFWorkbook) {
				final HSSFDataFormat format = ((HSSFWorkbook) wb)
						.createDataFormat();
				cell = ((HSSFRow) row).createCell(colIndex);
				final HSSFCellStyle style = ((HSSFWorkbook) wb).createCellStyle();
				style.cloneStyleFrom((HSSFCellStyle) cellStyle);
				style.setDataFormat(format.getFormat(dateFormat));

				((HSSFCell) cell).setCellValue(value);
				((HSSFCell) cell).setCellStyle(style);
			}
			if (wb instanceof org.apache.poi.xssf.usermodel.XSSFWorkbook) {
				final XSSFDataFormat format = ((XSSFWorkbook) wb)
						.createDataFormat();
				cell = ((XSSFRow) row).createCell(colIndex);
				final XSSFCellStyle style = ((XSSFWorkbook) wb).createCellStyle();
				style.cloneStyleFrom((XSSFCellStyle) cellStyle);
				style.setDataFormat(format.getFormat(dateFormat));

				((XSSFCell) cell).setCellValue(value);
				((XSSFCell) cell).setCellStyle(style);
			}
			return cell;
		}

		/**
		 * 创建单元格
		 *
		 * @param row
		 *            单元格所在的行
		 * @param colIndex
		 *            列号
		 * @param value
		 *            单元格填充日期值
		 * @param cellStyle
		 *            单元格样式
		 * @return 新生成的单元格
		 */
		public static Object createCell(final Object row, final int colIndex,
				final Date value, final Object cellStyle) {
			Object cell = null;
			if (row instanceof HSSFRow) {
				final HSSFRow objRow = (HSSFRow) row;
				cell = objRow.createCell(colIndex);
				final HSSFCellStyle objCellStyle = (HSSFCellStyle) cellStyle;
				((HSSFCell) cell).setCellStyle(objCellStyle);
				((HSSFCell) cell).setCellValue(value);
			}
			if (row instanceof org.apache.poi.xssf.usermodel.XSSFRow) {
				final XSSFRow objRow = (XSSFRow) row;
				cell = objRow.createCell(colIndex);
				final XSSFCellStyle objCellStyle = (XSSFCellStyle) cellStyle;
				((XSSFCell) cell).setCellStyle(objCellStyle);
				((XSSFCell) cell).setCellValue(value);
			}
			return cell;
		}

		/**
		 * 创建公式单元格
		 *
		 * @param row
		 *            单元格所在的行
		 * @param colIndex
		 *            列号
		 * @param formulaString
		 *            单元格填充公式
		 * @return 新生成的单元格
		 */
		public static Object createFormulaCell(final Object row,
				final int colIndex, final String formulaString) {
			Object cell = null;
			if (row instanceof HSSFRow) {
				final HSSFRow hssWb = (HSSFRow) row;
				cell = hssWb.createCell(colIndex);
				((HSSFCell) cell).setCellFormula(formulaString);
			}
			if (row instanceof org.apache.poi.xssf.usermodel.XSSFRow) {
				final XSSFRow xssWb = (XSSFRow) row;
				cell = xssWb.createCell(colIndex);
				((XSSFCell) cell).setCellFormula(formulaString);
			}
			return cell;
		}

		/**
		 * 创建公式单元格
		 *
		 * @param row
		 *            单元格所在的行
		 * @param colIndex
		 *            列号
		 * @param formulaString
		 *            单元格填充公式
		 * @param cellStyle
		 *            单元格样式
		 * @return 新生成的单元格
		 */
		public static Object createFormulaCell(final Object row,
				final int colIndex, final String formulaString,
				final Object cellStyle) {
			Object cell = null;
			if (row instanceof HSSFRow) {
				final HSSFRow hssWb = (HSSFRow) row;
				cell = hssWb.createCell(colIndex);
				((HSSFCell) cell).setCellFormula(formulaString);
				final HSSFCellStyle objCellStyle = (HSSFCellStyle) cellStyle;
				((HSSFCell) cell).setCellStyle(objCellStyle);
			}
			if (row instanceof org.apache.poi.xssf.usermodel.XSSFRow) {
				final XSSFRow xssWb = (XSSFRow) row;
				cell = xssWb.createCell(colIndex);
				((XSSFCell) cell).setCellFormula(formulaString);
				final XSSFCellStyle objCellStyle = (XSSFCellStyle) cellStyle;
				((XSSFCell) cell).setCellStyle(objCellStyle);
			}
			return cell;
		}

		/**
		 * 合并单元格
		 *
		 * @param rowFrom
		 *            开始行号
		 * @param colFrom
		 *            开始列号
		 * @param rowTo
		 *            结束行号
		 * @param colTo
		 *            结束列号
		 */
		public static void mergeRegion(final Object sheet, final int rowFrom,
				final int colFrom, final int rowTo, final int colTo) {

			if (sheet instanceof HSSFSheet) {
				((HSSFSheet) sheet).addMergedRegion(new Region(rowFrom,
						(short) colFrom, rowTo, (short) colTo));
			}
			if (sheet instanceof org.apache.poi.xssf.usermodel.XSSFSheet) {
				((XSSFSheet) sheet).addMergedRegion(new CellRangeAddress(rowFrom,
						colFrom, rowTo, colTo));
			}
		}

		public static Object getCStyleTemp(final Object wb) {
			Object cellStyle = null;
			if (wb instanceof HSSFWorkbook) {
				final HSSFWorkbook hssWb = (HSSFWorkbook) wb;
				// 创建单元格样式
				cellStyle = hssWb.createCellStyle();
				// 指定单元格居中对齐
				((HSSFCellStyle) cellStyle)
						.setAlignment(HSSFCellStyle.ALIGN_CENTER);

				// 指定单元格垂直居中对齐
				((HSSFCellStyle) cellStyle)
						.setVerticalAlignment(HSSFCellStyle.VERTICAL_CENTER);

				// 指定当单元格内容显示不下时自动换行
				((HSSFCellStyle) cellStyle).setWrapText(true);

				// 设置单元格字体
				final HSSFFont font = hssWb.createFont();
				font.setBoldweight(HSSFFont.BOLDWEIGHT_BOLD);
				font.setFontName("宋体");
				font.setFontHeight((short) 200);
				((HSSFCellStyle) cellStyle).setFont(font);
			}
			if (wb instanceof XSSFWorkbook) {
				final XSSFWorkbook xssWb = (XSSFWorkbook) wb;
				// 创建单元格样式
				cellStyle = xssWb.createCellStyle();
				// 指定单元格居中对齐
				((XSSFCellStyle) cellStyle)
						.setAlignment(XSSFCellStyle.ALIGN_CENTER);

				// 指定单元格垂直居中对齐
				((XSSFCellStyle) cellStyle)
						.setVerticalAlignment(XSSFCellStyle.VERTICAL_CENTER);

				// 指定当单元格内容显示不下时自动换行
				((XSSFCellStyle) cellStyle).setWrapText(true);

				// 设置单元格字体
				final XSSFFont font = xssWb.createFont();
				font.setBoldweight(XSSFFont.BOLDWEIGHT_BOLD);
				font.setFontName("宋体");
				font.setFontHeight((short) 200);
				((XSSFCellStyle) cellStyle).setFont(font);
			}

			return cellStyle;
		}

		/** 提供转换编码后的供下载用的文件名 */

		public static String getDownloadFileName(final String fileName) {

			String downFileName = fileName;
			if (downFileName.length() < 1 || downFileName.equals("undefined")) {
				downFileName = fileName;
			}
			try {
				downFileName = new String(downFileName.getBytes(), "ISO8859-1");

			} catch (final UnsupportedEncodingException ignore) {
			}

			return downFileName;
		}

		/** 获取全边框样式 */
		public static Object getBorderCellStyle(final Object wb) {
			Object cellStyle = null;
			if (wb instanceof HSSFWorkbook) {
				final HSSFWorkbook hssWb = (HSSFWorkbook) wb;
				// 创建单元格样式
				cellStyle = hssWb.createCellStyle();

				((HSSFCellStyle) cellStyle)
						.setBorderBottom(HSSFCellStyle.BORDER_THIN);
				((HSSFCellStyle) cellStyle)
						.setBorderLeft(HSSFCellStyle.BORDER_THIN);
				((HSSFCellStyle) cellStyle)
						.setBorderRight(HSSFCellStyle.BORDER_THIN);
				((HSSFCellStyle) cellStyle).setBorderTop(HSSFCellStyle.BORDER_THIN);
			}
			if (wb instanceof XSSFWorkbook) {
				final XSSFWorkbook xssWb = (XSSFWorkbook) wb;
				// 创建单元格样式
				cellStyle = xssWb.createCellStyle();

				((XSSFCellStyle) cellStyle)
						.setBorderBottom(XSSFCellStyle.BORDER_THIN);
				((XSSFCellStyle) cellStyle)
						.setBorderLeft(XSSFCellStyle.BORDER_THIN);
				((XSSFCellStyle) cellStyle)
						.setBorderRight(XSSFCellStyle.BORDER_THIN);
				((XSSFCellStyle) cellStyle).setBorderTop(XSSFCellStyle.BORDER_THIN);
			}
			return cellStyle;

		}
}
