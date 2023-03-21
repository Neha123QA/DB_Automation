import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class test1 {

	Connection con=null;
	Statement sta=null;
	ResultSet rs,rs1,rs2=null;
	CallableStatement cstat;

	@BeforeClass
	public void setup() throws SQLException {
		con=DriverManager.getConnection("jdbc:mysql://localhost:3306/classicmodels","root","root");
	}

	@AfterClass
	public void teardown() throws SQLException {
		con.close();
	}


	@Test public void testcase1() throws SQLException {
		sta=con.createStatement();
		rs=sta.executeQuery("show procedure status where name='SelectAllCustomers';"
				); rs.next();

				Assert.assertEquals(rs.getString("Name"), "SelectAllCustomers"); }

	@Test(priority=2) public void testcase2() throws SQLException {
		cstat=con.prepareCall("{call SelectAllCustomers()}");
		rs1=cstat.executeQuery();

		sta=con.createStatement(); 
		rs2=sta.executeQuery("select * from customers;");

		Assert.assertEquals(compare_DBtable(rs1, rs2),true);

	}


	@Test(priority=3)
	public void testcase3() throws SQLException {
		cstat=con.prepareCall("{call SelectAllCustomeresbycity(?)}");
		cstat.setString(1, "Singapore");
		rs1=cstat.executeQuery();

		sta=con.createStatement();
		rs2=sta.executeQuery("select * from customers where city='Singapore';");

		Assert.assertEquals(compare_DBtable(rs1, rs2),true);

	}

	@Test(priority=4)
	public void testcase4() throws SQLException {
		cstat=con.prepareCall("{call SelectAllCustomersBycityandpcode(?,?)}");
		cstat.setString(1, "Singapore");
		cstat.setString(2, "079903");
		rs1=cstat.executeQuery();

		sta=con.createStatement();
		rs2=sta.executeQuery("select * from customers where city='singapore' and postalCode='079903';");

		Assert.assertEquals(compare_DBtable(rs1, rs2),true);

	}

	@Test
	public void testcase5() throws SQLException {
		cstat=con.prepareCall("call get_order_cust(?,?,?,?,?);");
		cstat.setInt(1, 141);
		cstat.registerOutParameter(2, Types.INTEGER);
		cstat.registerOutParameter(3, Types.INTEGER);
		cstat.registerOutParameter(4, Types.INTEGER);
		cstat.registerOutParameter(5, Types.INTEGER);

		cstat.executeQuery();
		int Shipped=cstat.getInt(2);
		int Canceled=cstat.getInt(3);
		int Resolved=cstat.getInt(4);
		int Disputed=cstat.getInt(5);

		System.out.println(Shipped +" "+Canceled +" "+Resolved+" "+Disputed);

		sta=con.createStatement();
		rs=sta.executeQuery("select (select count(*) as 'shipped' from orders where customerNumber=141 and status='shipped') as Shipped , (select count(*) as 'canceled' from orders where customerNumber=141 and status='canceled') as Canceled, (select count(*) as 'resolved' from orders where customerNumber=141 and status='resolved') as Resolved ,(select count(*) as 'disputed' from orders where customerNumber=141 and status='disputed') as Disputed");
		rs.next();
		
		int exp_Shipped=rs.getInt("shipped");
		int exp_Canceled=rs.getInt("canceled");
		int exp_Resolved=rs.getInt("resolved");
		int exp_Disputed=rs.getInt("disputed");

		if(Shipped==exp_Shipped && Canceled==exp_Canceled && Resolved==exp_Resolved && Disputed==exp_Disputed) {
			Assert.assertTrue(true);
		}
		else
			Assert.assertTrue(false);
	}
	
	@Test
	public void testcase6() throws SQLException {
		
		cstat=con.prepareCall("call getcustomershipping(?,?);");
		cstat.setInt(1, 121);
		cstat.registerOutParameter(2, Types.VARCHAR);
		cstat.executeQuery();
		String shippinttime=cstat.getString(2);
		
		sta=con.createStatement();
		rs=sta.executeQuery("select country,\r\n"
				+ "case \r\n"
				+ "when country='USA' then '2-day Shipping'\r\n"
				+ "when country='Canada'then '3-day Shipping'\r\n"
				+ "else '5-day Shipping'\r\n"
				+ "end as shippingtime\r\n"
				+ "from customers where customerNumber=121;");
		rs.next();
		String exp_shippingtime=rs.getString("shippingtime");
		
		Assert.assertEquals(shippinttime, exp_shippingtime);
	}

	public boolean compare_DBtable(ResultSet resultset1, ResultSet resultset2) throws SQLException {
		while(resultset1.next()){
			resultset2.next();
			int cou=resultset1.getMetaData().getColumnCount();
			for(int i=1;i<=cou;i++) {
				if(!StringUtils.equals(resultset1.getString(i), resultset2.getString(i))){
					return false;
				}
			}
		}
		return true;
	}


}
