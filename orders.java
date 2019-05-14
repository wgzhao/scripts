// ORM class for table 'orders'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Tue Nov 22 11:31:12 CST 2016
// For connector: org.apache.sqoop.manager.MySQLManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class orders extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  protected ResultSet __cur_result_set;
  private Integer rowid;
  public Integer get_rowid() {
    return rowid;
  }
  public void set_rowid(Integer rowid) {
    this.rowid = rowid;
  }
  public orders with_rowid(Integer rowid) {
    this.rowid = rowid;
    return this;
  }
  private String order_id;
  public String get_order_id() {
    return order_id;
  }
  public void set_order_id(String order_id) {
    this.order_id = order_id;
  }
  public orders with_order_id(String order_id) {
    this.order_id = order_id;
    return this;
  }
  private java.sql.Date order_date;
  public java.sql.Date get_order_date() {
    return order_date;
  }
  public void set_order_date(java.sql.Date order_date) {
    this.order_date = order_date;
  }
  public orders with_order_date(java.sql.Date order_date) {
    this.order_date = order_date;
    return this;
  }
  private java.sql.Date delivery_date;
  public java.sql.Date get_delivery_date() {
    return delivery_date;
  }
  public void set_delivery_date(java.sql.Date delivery_date) {
    this.delivery_date = delivery_date;
  }
  public orders with_delivery_date(java.sql.Date delivery_date) {
    this.delivery_date = delivery_date;
    return this;
  }
  private String post_type;
  public String get_post_type() {
    return post_type;
  }
  public void set_post_type(String post_type) {
    this.post_type = post_type;
  }
  public orders with_post_type(String post_type) {
    this.post_type = post_type;
    return this;
  }
  private Integer customer_id;
  public Integer get_customer_id() {
    return customer_id;
  }
  public void set_customer_id(Integer customer_id) {
    this.customer_id = customer_id;
  }
  public orders with_customer_id(Integer customer_id) {
    this.customer_id = customer_id;
    return this;
  }
  private String customer_name;
  public String get_customer_name() {
    return customer_name;
  }
  public void set_customer_name(String customer_name) {
    this.customer_name = customer_name;
  }
  public orders with_customer_name(String customer_name) {
    this.customer_name = customer_name;
    return this;
  }
  private String customer_type;
  public String get_customer_type() {
    return customer_type;
  }
  public void set_customer_type(String customer_type) {
    this.customer_type = customer_type;
  }
  public orders with_customer_type(String customer_type) {
    this.customer_type = customer_type;
    return this;
  }
  private String city;
  public String get_city() {
    return city;
  }
  public void set_city(String city) {
    this.city = city;
  }
  public orders with_city(String city) {
    this.city = city;
    return this;
  }
  private String province;
  public String get_province() {
    return province;
  }
  public void set_province(String province) {
    this.province = province;
  }
  public orders with_province(String province) {
    this.province = province;
    return this;
  }
  private String country;
  public String get_country() {
    return country;
  }
  public void set_country(String country) {
    this.country = country;
  }
  public orders with_country(String country) {
    this.country = country;
    return this;
  }
  private String area;
  public String get_area() {
    return area;
  }
  public void set_area(String area) {
    this.area = area;
  }
  public orders with_area(String area) {
    this.area = area;
    return this;
  }
  private String product_id;
  public String get_product_id() {
    return product_id;
  }
  public void set_product_id(String product_id) {
    this.product_id = product_id;
  }
  public orders with_product_id(String product_id) {
    this.product_id = product_id;
    return this;
  }
  private String product_type;
  public String get_product_type() {
    return product_type;
  }
  public void set_product_type(String product_type) {
    this.product_type = product_type;
  }
  public orders with_product_type(String product_type) {
    this.product_type = product_type;
    return this;
  }
  private String product_subtype;
  public String get_product_subtype() {
    return product_subtype;
  }
  public void set_product_subtype(String product_subtype) {
    this.product_subtype = product_subtype;
  }
  public orders with_product_subtype(String product_subtype) {
    this.product_subtype = product_subtype;
    return this;
  }
  private String product_name;
  public String get_product_name() {
    return product_name;
  }
  public void set_product_name(String product_name) {
    this.product_name = product_name;
  }
  public orders with_product_name(String product_name) {
    this.product_name = product_name;
    return this;
  }
  private Float sale_volume;
  public Float get_sale_volume() {
    return sale_volume;
  }
  public void set_sale_volume(Float sale_volume) {
    this.sale_volume = sale_volume;
  }
  public orders with_sale_volume(Float sale_volume) {
    this.sale_volume = sale_volume;
    return this;
  }
  private Integer quantity;
  public Integer get_quantity() {
    return quantity;
  }
  public void set_quantity(Integer quantity) {
    this.quantity = quantity;
  }
  public orders with_quantity(Integer quantity) {
    this.quantity = quantity;
    return this;
  }
  private Float discount;
  public Float get_discount() {
    return discount;
  }
  public void set_discount(Float discount) {
    this.discount = discount;
  }
  public orders with_discount(Float discount) {
    this.discount = discount;
    return this;
  }
  private java.math.BigDecimal profits;
  public java.math.BigDecimal get_profits() {
    return profits;
  }
  public void set_profits(java.math.BigDecimal profits) {
    this.profits = profits;
  }
  public orders with_profits(java.math.BigDecimal profits) {
    this.profits = profits;
    return this;
  }
  private String returned;
  public String get_returned() {
    return returned;
  }
  public void set_returned(String returned) {
    this.returned = returned;
  }
  public orders with_returned(String returned) {
    this.returned = returned;
    return this;
  }
  private String sale;
  public String get_sale() {
    return sale;
  }
  public void set_sale(String sale) {
    this.sale = sale;
  }
  public orders with_sale(String sale) {
    this.sale = sale;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof orders)) {
      return false;
    }
    orders that = (orders) o;
    boolean equal = true;
    equal = equal && (this.rowid == null ? that.rowid == null : this.rowid.equals(that.rowid));
    equal = equal && (this.order_id == null ? that.order_id == null : this.order_id.equals(that.order_id));
    equal = equal && (this.order_date == null ? that.order_date == null : this.order_date.equals(that.order_date));
    equal = equal && (this.delivery_date == null ? that.delivery_date == null : this.delivery_date.equals(that.delivery_date));
    equal = equal && (this.post_type == null ? that.post_type == null : this.post_type.equals(that.post_type));
    equal = equal && (this.customer_id == null ? that.customer_id == null : this.customer_id.equals(that.customer_id));
    equal = equal && (this.customer_name == null ? that.customer_name == null : this.customer_name.equals(that.customer_name));
    equal = equal && (this.customer_type == null ? that.customer_type == null : this.customer_type.equals(that.customer_type));
    equal = equal && (this.city == null ? that.city == null : this.city.equals(that.city));
    equal = equal && (this.province == null ? that.province == null : this.province.equals(that.province));
    equal = equal && (this.country == null ? that.country == null : this.country.equals(that.country));
    equal = equal && (this.area == null ? that.area == null : this.area.equals(that.area));
    equal = equal && (this.product_id == null ? that.product_id == null : this.product_id.equals(that.product_id));
    equal = equal && (this.product_type == null ? that.product_type == null : this.product_type.equals(that.product_type));
    equal = equal && (this.product_subtype == null ? that.product_subtype == null : this.product_subtype.equals(that.product_subtype));
    equal = equal && (this.product_name == null ? that.product_name == null : this.product_name.equals(that.product_name));
    equal = equal && (this.sale_volume == null ? that.sale_volume == null : this.sale_volume.equals(that.sale_volume));
    equal = equal && (this.quantity == null ? that.quantity == null : this.quantity.equals(that.quantity));
    equal = equal && (this.discount == null ? that.discount == null : this.discount.equals(that.discount));
    equal = equal && (this.profits == null ? that.profits == null : this.profits.equals(that.profits));
    equal = equal && (this.returned == null ? that.returned == null : this.returned.equals(that.returned));
    equal = equal && (this.sale == null ? that.sale == null : this.sale.equals(that.sale));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof orders)) {
      return false;
    }
    orders that = (orders) o;
    boolean equal = true;
    equal = equal && (this.rowid == null ? that.rowid == null : this.rowid.equals(that.rowid));
    equal = equal && (this.order_id == null ? that.order_id == null : this.order_id.equals(that.order_id));
    equal = equal && (this.order_date == null ? that.order_date == null : this.order_date.equals(that.order_date));
    equal = equal && (this.delivery_date == null ? that.delivery_date == null : this.delivery_date.equals(that.delivery_date));
    equal = equal && (this.post_type == null ? that.post_type == null : this.post_type.equals(that.post_type));
    equal = equal && (this.customer_id == null ? that.customer_id == null : this.customer_id.equals(that.customer_id));
    equal = equal && (this.customer_name == null ? that.customer_name == null : this.customer_name.equals(that.customer_name));
    equal = equal && (this.customer_type == null ? that.customer_type == null : this.customer_type.equals(that.customer_type));
    equal = equal && (this.city == null ? that.city == null : this.city.equals(that.city));
    equal = equal && (this.province == null ? that.province == null : this.province.equals(that.province));
    equal = equal && (this.country == null ? that.country == null : this.country.equals(that.country));
    equal = equal && (this.area == null ? that.area == null : this.area.equals(that.area));
    equal = equal && (this.product_id == null ? that.product_id == null : this.product_id.equals(that.product_id));
    equal = equal && (this.product_type == null ? that.product_type == null : this.product_type.equals(that.product_type));
    equal = equal && (this.product_subtype == null ? that.product_subtype == null : this.product_subtype.equals(that.product_subtype));
    equal = equal && (this.product_name == null ? that.product_name == null : this.product_name.equals(that.product_name));
    equal = equal && (this.sale_volume == null ? that.sale_volume == null : this.sale_volume.equals(that.sale_volume));
    equal = equal && (this.quantity == null ? that.quantity == null : this.quantity.equals(that.quantity));
    equal = equal && (this.discount == null ? that.discount == null : this.discount.equals(that.discount));
    equal = equal && (this.profits == null ? that.profits == null : this.profits.equals(that.profits));
    equal = equal && (this.returned == null ? that.returned == null : this.returned.equals(that.returned));
    equal = equal && (this.sale == null ? that.sale == null : this.sale.equals(that.sale));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.rowid = JdbcWritableBridge.readInteger(1, __dbResults);
    this.order_id = JdbcWritableBridge.readString(2, __dbResults);
    this.order_date = JdbcWritableBridge.readDate(3, __dbResults);
    this.delivery_date = JdbcWritableBridge.readDate(4, __dbResults);
    this.post_type = JdbcWritableBridge.readString(5, __dbResults);
    this.customer_id = JdbcWritableBridge.readInteger(6, __dbResults);
    this.customer_name = JdbcWritableBridge.readString(7, __dbResults);
    this.customer_type = JdbcWritableBridge.readString(8, __dbResults);
    this.city = JdbcWritableBridge.readString(9, __dbResults);
    this.province = JdbcWritableBridge.readString(10, __dbResults);
    this.country = JdbcWritableBridge.readString(11, __dbResults);
    this.area = JdbcWritableBridge.readString(12, __dbResults);
    this.product_id = JdbcWritableBridge.readString(13, __dbResults);
    this.product_type = JdbcWritableBridge.readString(14, __dbResults);
    this.product_subtype = JdbcWritableBridge.readString(15, __dbResults);
    this.product_name = JdbcWritableBridge.readString(16, __dbResults);
    this.sale_volume = JdbcWritableBridge.readFloat(17, __dbResults);
    this.quantity = JdbcWritableBridge.readInteger(18, __dbResults);
    this.discount = JdbcWritableBridge.readFloat(19, __dbResults);
    this.profits = JdbcWritableBridge.readBigDecimal(20, __dbResults);
    this.returned = JdbcWritableBridge.readString(21, __dbResults);
    this.sale = JdbcWritableBridge.readString(22, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.rowid = JdbcWritableBridge.readInteger(1, __dbResults);
    this.order_id = JdbcWritableBridge.readString(2, __dbResults);
    this.order_date = JdbcWritableBridge.readDate(3, __dbResults);
    this.delivery_date = JdbcWritableBridge.readDate(4, __dbResults);
    this.post_type = JdbcWritableBridge.readString(5, __dbResults);
    this.customer_id = JdbcWritableBridge.readInteger(6, __dbResults);
    this.customer_name = JdbcWritableBridge.readString(7, __dbResults);
    this.customer_type = JdbcWritableBridge.readString(8, __dbResults);
    this.city = JdbcWritableBridge.readString(9, __dbResults);
    this.province = JdbcWritableBridge.readString(10, __dbResults);
    this.country = JdbcWritableBridge.readString(11, __dbResults);
    this.area = JdbcWritableBridge.readString(12, __dbResults);
    this.product_id = JdbcWritableBridge.readString(13, __dbResults);
    this.product_type = JdbcWritableBridge.readString(14, __dbResults);
    this.product_subtype = JdbcWritableBridge.readString(15, __dbResults);
    this.product_name = JdbcWritableBridge.readString(16, __dbResults);
    this.sale_volume = JdbcWritableBridge.readFloat(17, __dbResults);
    this.quantity = JdbcWritableBridge.readInteger(18, __dbResults);
    this.discount = JdbcWritableBridge.readFloat(19, __dbResults);
    this.profits = JdbcWritableBridge.readBigDecimal(20, __dbResults);
    this.returned = JdbcWritableBridge.readString(21, __dbResults);
    this.sale = JdbcWritableBridge.readString(22, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(rowid, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(order_id, 2 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDate(order_date, 3 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeDate(delivery_date, 4 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeString(post_type, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(customer_id, 6 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(customer_name, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(customer_type, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(city, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(province, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(country, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(area, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(product_id, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(product_type, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(product_subtype, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(product_name, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeFloat(sale_volume, 17 + __off, 7, __dbStmt);
    JdbcWritableBridge.writeInteger(quantity, 18 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeFloat(discount, 19 + __off, 7, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(profits, 20 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(returned, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(sale, 22 + __off, 12, __dbStmt);
    return 22;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(rowid, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(order_id, 2 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDate(order_date, 3 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeDate(delivery_date, 4 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeString(post_type, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(customer_id, 6 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(customer_name, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(customer_type, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(city, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(province, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(country, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(area, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(product_id, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(product_type, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(product_subtype, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(product_name, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeFloat(sale_volume, 17 + __off, 7, __dbStmt);
    JdbcWritableBridge.writeInteger(quantity, 18 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeFloat(discount, 19 + __off, 7, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(profits, 20 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(returned, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(sale, 22 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.rowid = null;
    } else {
    this.rowid = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.order_id = null;
    } else {
    this.order_id = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.order_date = null;
    } else {
    this.order_date = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.delivery_date = null;
    } else {
    this.delivery_date = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.post_type = null;
    } else {
    this.post_type = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.customer_id = null;
    } else {
    this.customer_id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.customer_name = null;
    } else {
    this.customer_name = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.customer_type = null;
    } else {
    this.customer_type = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.city = null;
    } else {
    this.city = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.province = null;
    } else {
    this.province = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.country = null;
    } else {
    this.country = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.area = null;
    } else {
    this.area = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.product_id = null;
    } else {
    this.product_id = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.product_type = null;
    } else {
    this.product_type = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.product_subtype = null;
    } else {
    this.product_subtype = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.product_name = null;
    } else {
    this.product_name = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.sale_volume = null;
    } else {
    this.sale_volume = Float.valueOf(__dataIn.readFloat());
    }
    if (__dataIn.readBoolean()) { 
        this.quantity = null;
    } else {
    this.quantity = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.discount = null;
    } else {
    this.discount = Float.valueOf(__dataIn.readFloat());
    }
    if (__dataIn.readBoolean()) { 
        this.profits = null;
    } else {
    this.profits = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.returned = null;
    } else {
    this.returned = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.sale = null;
    } else {
    this.sale = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.rowid) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.rowid);
    }
    if (null == this.order_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, order_id);
    }
    if (null == this.order_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.order_date.getTime());
    }
    if (null == this.delivery_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.delivery_date.getTime());
    }
    if (null == this.post_type) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, post_type);
    }
    if (null == this.customer_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.customer_id);
    }
    if (null == this.customer_name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, customer_name);
    }
    if (null == this.customer_type) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, customer_type);
    }
    if (null == this.city) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, city);
    }
    if (null == this.province) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, province);
    }
    if (null == this.country) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, country);
    }
    if (null == this.area) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, area);
    }
    if (null == this.product_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, product_id);
    }
    if (null == this.product_type) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, product_type);
    }
    if (null == this.product_subtype) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, product_subtype);
    }
    if (null == this.product_name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, product_name);
    }
    if (null == this.sale_volume) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeFloat(this.sale_volume);
    }
    if (null == this.quantity) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.quantity);
    }
    if (null == this.discount) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeFloat(this.discount);
    }
    if (null == this.profits) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.profits, __dataOut);
    }
    if (null == this.returned) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, returned);
    }
    if (null == this.sale) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, sale);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.rowid) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.rowid);
    }
    if (null == this.order_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, order_id);
    }
    if (null == this.order_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.order_date.getTime());
    }
    if (null == this.delivery_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.delivery_date.getTime());
    }
    if (null == this.post_type) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, post_type);
    }
    if (null == this.customer_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.customer_id);
    }
    if (null == this.customer_name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, customer_name);
    }
    if (null == this.customer_type) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, customer_type);
    }
    if (null == this.city) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, city);
    }
    if (null == this.province) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, province);
    }
    if (null == this.country) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, country);
    }
    if (null == this.area) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, area);
    }
    if (null == this.product_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, product_id);
    }
    if (null == this.product_type) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, product_type);
    }
    if (null == this.product_subtype) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, product_subtype);
    }
    if (null == this.product_name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, product_name);
    }
    if (null == this.sale_volume) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeFloat(this.sale_volume);
    }
    if (null == this.quantity) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.quantity);
    }
    if (null == this.discount) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeFloat(this.discount);
    }
    if (null == this.profits) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.profits, __dataOut);
    }
    if (null == this.returned) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, returned);
    }
    if (null == this.sale) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, sale);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(rowid==null?"null":"" + rowid, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(order_id==null?"null":order_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(order_date==null?"null":"" + order_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(delivery_date==null?"null":"" + delivery_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(post_type==null?"null":post_type, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(customer_id==null?"null":"" + customer_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(customer_name==null?"null":customer_name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(customer_type==null?"null":customer_type, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(city==null?"null":city, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(province==null?"null":province, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(country==null?"null":country, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(area==null?"null":area, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(product_id==null?"null":product_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(product_type==null?"null":product_type, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(product_subtype==null?"null":product_subtype, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(product_name==null?"null":product_name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(sale_volume==null?"null":"" + sale_volume, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(quantity==null?"null":"" + quantity, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(discount==null?"null":"" + discount, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(profits==null?"null":profits.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(returned==null?"null":returned, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(sale==null?"null":sale, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(rowid==null?"null":"" + rowid, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(order_id==null?"null":order_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(order_date==null?"null":"" + order_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(delivery_date==null?"null":"" + delivery_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(post_type==null?"null":post_type, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(customer_id==null?"null":"" + customer_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(customer_name==null?"null":customer_name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(customer_type==null?"null":customer_type, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(city==null?"null":city, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(province==null?"null":province, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(country==null?"null":country, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(area==null?"null":area, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(product_id==null?"null":product_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(product_type==null?"null":product_type, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(product_subtype==null?"null":product_subtype, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(product_name==null?"null":product_name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(sale_volume==null?"null":"" + sale_volume, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(quantity==null?"null":"" + quantity, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(discount==null?"null":"" + discount, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(profits==null?"null":profits.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(returned==null?"null":returned, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(sale==null?"null":sale, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.rowid = null; } else {
      this.rowid = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.order_id = null; } else {
      this.order_id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.order_date = null; } else {
      this.order_date = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.delivery_date = null; } else {
      this.delivery_date = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.post_type = null; } else {
      this.post_type = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.customer_id = null; } else {
      this.customer_id = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.customer_name = null; } else {
      this.customer_name = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.customer_type = null; } else {
      this.customer_type = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.city = null; } else {
      this.city = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.province = null; } else {
      this.province = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.country = null; } else {
      this.country = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.area = null; } else {
      this.area = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.product_id = null; } else {
      this.product_id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.product_type = null; } else {
      this.product_type = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.product_subtype = null; } else {
      this.product_subtype = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.product_name = null; } else {
      this.product_name = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.sale_volume = null; } else {
      this.sale_volume = Float.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.quantity = null; } else {
      this.quantity = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.discount = null; } else {
      this.discount = Float.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.profits = null; } else {
      this.profits = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.returned = null; } else {
      this.returned = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.sale = null; } else {
      this.sale = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.rowid = null; } else {
      this.rowid = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.order_id = null; } else {
      this.order_id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.order_date = null; } else {
      this.order_date = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.delivery_date = null; } else {
      this.delivery_date = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.post_type = null; } else {
      this.post_type = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.customer_id = null; } else {
      this.customer_id = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.customer_name = null; } else {
      this.customer_name = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.customer_type = null; } else {
      this.customer_type = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.city = null; } else {
      this.city = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.province = null; } else {
      this.province = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.country = null; } else {
      this.country = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.area = null; } else {
      this.area = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.product_id = null; } else {
      this.product_id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.product_type = null; } else {
      this.product_type = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.product_subtype = null; } else {
      this.product_subtype = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.product_name = null; } else {
      this.product_name = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.sale_volume = null; } else {
      this.sale_volume = Float.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.quantity = null; } else {
      this.quantity = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.discount = null; } else {
      this.discount = Float.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.profits = null; } else {
      this.profits = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.returned = null; } else {
      this.returned = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.sale = null; } else {
      this.sale = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    orders o = (orders) super.clone();
    o.order_date = (o.order_date != null) ? (java.sql.Date) o.order_date.clone() : null;
    o.delivery_date = (o.delivery_date != null) ? (java.sql.Date) o.delivery_date.clone() : null;
    return o;
  }

  public void clone0(orders o) throws CloneNotSupportedException {
    o.order_date = (o.order_date != null) ? (java.sql.Date) o.order_date.clone() : null;
    o.delivery_date = (o.delivery_date != null) ? (java.sql.Date) o.delivery_date.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new TreeMap<String, Object>();
    __sqoop$field_map.put("rowid", this.rowid);
    __sqoop$field_map.put("order_id", this.order_id);
    __sqoop$field_map.put("order_date", this.order_date);
    __sqoop$field_map.put("delivery_date", this.delivery_date);
    __sqoop$field_map.put("post_type", this.post_type);
    __sqoop$field_map.put("customer_id", this.customer_id);
    __sqoop$field_map.put("customer_name", this.customer_name);
    __sqoop$field_map.put("customer_type", this.customer_type);
    __sqoop$field_map.put("city", this.city);
    __sqoop$field_map.put("province", this.province);
    __sqoop$field_map.put("country", this.country);
    __sqoop$field_map.put("area", this.area);
    __sqoop$field_map.put("product_id", this.product_id);
    __sqoop$field_map.put("product_type", this.product_type);
    __sqoop$field_map.put("product_subtype", this.product_subtype);
    __sqoop$field_map.put("product_name", this.product_name);
    __sqoop$field_map.put("sale_volume", this.sale_volume);
    __sqoop$field_map.put("quantity", this.quantity);
    __sqoop$field_map.put("discount", this.discount);
    __sqoop$field_map.put("profits", this.profits);
    __sqoop$field_map.put("returned", this.returned);
    __sqoop$field_map.put("sale", this.sale);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("rowid", this.rowid);
    __sqoop$field_map.put("order_id", this.order_id);
    __sqoop$field_map.put("order_date", this.order_date);
    __sqoop$field_map.put("delivery_date", this.delivery_date);
    __sqoop$field_map.put("post_type", this.post_type);
    __sqoop$field_map.put("customer_id", this.customer_id);
    __sqoop$field_map.put("customer_name", this.customer_name);
    __sqoop$field_map.put("customer_type", this.customer_type);
    __sqoop$field_map.put("city", this.city);
    __sqoop$field_map.put("province", this.province);
    __sqoop$field_map.put("country", this.country);
    __sqoop$field_map.put("area", this.area);
    __sqoop$field_map.put("product_id", this.product_id);
    __sqoop$field_map.put("product_type", this.product_type);
    __sqoop$field_map.put("product_subtype", this.product_subtype);
    __sqoop$field_map.put("product_name", this.product_name);
    __sqoop$field_map.put("sale_volume", this.sale_volume);
    __sqoop$field_map.put("quantity", this.quantity);
    __sqoop$field_map.put("discount", this.discount);
    __sqoop$field_map.put("profits", this.profits);
    __sqoop$field_map.put("returned", this.returned);
    __sqoop$field_map.put("sale", this.sale);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if ("rowid".equals(__fieldName)) {
      this.rowid = (Integer) __fieldVal;
    }
    else    if ("order_id".equals(__fieldName)) {
      this.order_id = (String) __fieldVal;
    }
    else    if ("order_date".equals(__fieldName)) {
      this.order_date = (java.sql.Date) __fieldVal;
    }
    else    if ("delivery_date".equals(__fieldName)) {
      this.delivery_date = (java.sql.Date) __fieldVal;
    }
    else    if ("post_type".equals(__fieldName)) {
      this.post_type = (String) __fieldVal;
    }
    else    if ("customer_id".equals(__fieldName)) {
      this.customer_id = (Integer) __fieldVal;
    }
    else    if ("customer_name".equals(__fieldName)) {
      this.customer_name = (String) __fieldVal;
    }
    else    if ("customer_type".equals(__fieldName)) {
      this.customer_type = (String) __fieldVal;
    }
    else    if ("city".equals(__fieldName)) {
      this.city = (String) __fieldVal;
    }
    else    if ("province".equals(__fieldName)) {
      this.province = (String) __fieldVal;
    }
    else    if ("country".equals(__fieldName)) {
      this.country = (String) __fieldVal;
    }
    else    if ("area".equals(__fieldName)) {
      this.area = (String) __fieldVal;
    }
    else    if ("product_id".equals(__fieldName)) {
      this.product_id = (String) __fieldVal;
    }
    else    if ("product_type".equals(__fieldName)) {
      this.product_type = (String) __fieldVal;
    }
    else    if ("product_subtype".equals(__fieldName)) {
      this.product_subtype = (String) __fieldVal;
    }
    else    if ("product_name".equals(__fieldName)) {
      this.product_name = (String) __fieldVal;
    }
    else    if ("sale_volume".equals(__fieldName)) {
      this.sale_volume = (Float) __fieldVal;
    }
    else    if ("quantity".equals(__fieldName)) {
      this.quantity = (Integer) __fieldVal;
    }
    else    if ("discount".equals(__fieldName)) {
      this.discount = (Float) __fieldVal;
    }
    else    if ("profits".equals(__fieldName)) {
      this.profits = (java.math.BigDecimal) __fieldVal;
    }
    else    if ("returned".equals(__fieldName)) {
      this.returned = (String) __fieldVal;
    }
    else    if ("sale".equals(__fieldName)) {
      this.sale = (String) __fieldVal;
    }
    else {
      throw new RuntimeException("No such field: " + __fieldName);
    }
  }
  public boolean setField0(String __fieldName, Object __fieldVal) {
    if ("rowid".equals(__fieldName)) {
      this.rowid = (Integer) __fieldVal;
      return true;
    }
    else    if ("order_id".equals(__fieldName)) {
      this.order_id = (String) __fieldVal;
      return true;
    }
    else    if ("order_date".equals(__fieldName)) {
      this.order_date = (java.sql.Date) __fieldVal;
      return true;
    }
    else    if ("delivery_date".equals(__fieldName)) {
      this.delivery_date = (java.sql.Date) __fieldVal;
      return true;
    }
    else    if ("post_type".equals(__fieldName)) {
      this.post_type = (String) __fieldVal;
      return true;
    }
    else    if ("customer_id".equals(__fieldName)) {
      this.customer_id = (Integer) __fieldVal;
      return true;
    }
    else    if ("customer_name".equals(__fieldName)) {
      this.customer_name = (String) __fieldVal;
      return true;
    }
    else    if ("customer_type".equals(__fieldName)) {
      this.customer_type = (String) __fieldVal;
      return true;
    }
    else    if ("city".equals(__fieldName)) {
      this.city = (String) __fieldVal;
      return true;
    }
    else    if ("province".equals(__fieldName)) {
      this.province = (String) __fieldVal;
      return true;
    }
    else    if ("country".equals(__fieldName)) {
      this.country = (String) __fieldVal;
      return true;
    }
    else    if ("area".equals(__fieldName)) {
      this.area = (String) __fieldVal;
      return true;
    }
    else    if ("product_id".equals(__fieldName)) {
      this.product_id = (String) __fieldVal;
      return true;
    }
    else    if ("product_type".equals(__fieldName)) {
      this.product_type = (String) __fieldVal;
      return true;
    }
    else    if ("product_subtype".equals(__fieldName)) {
      this.product_subtype = (String) __fieldVal;
      return true;
    }
    else    if ("product_name".equals(__fieldName)) {
      this.product_name = (String) __fieldVal;
      return true;
    }
    else    if ("sale_volume".equals(__fieldName)) {
      this.sale_volume = (Float) __fieldVal;
      return true;
    }
    else    if ("quantity".equals(__fieldName)) {
      this.quantity = (Integer) __fieldVal;
      return true;
    }
    else    if ("discount".equals(__fieldName)) {
      this.discount = (Float) __fieldVal;
      return true;
    }
    else    if ("profits".equals(__fieldName)) {
      this.profits = (java.math.BigDecimal) __fieldVal;
      return true;
    }
    else    if ("returned".equals(__fieldName)) {
      this.returned = (String) __fieldVal;
      return true;
    }
    else    if ("sale".equals(__fieldName)) {
      this.sale = (String) __fieldVal;
      return true;
    }
    else {
      return false;    }
  }
}
