require 'test/unit'
require './lib/FluidDb'


class FormatSQLTest < Test::Unit::TestCase
    def test_no_params
        sql_in = "SELECT field1, field2 FROM table1"
        
        sql_out = FluidDb::Base.new.format_to_sql( sql_in )
        
        assert_equal sql_in, sql_out
    end
    
    def test_single_int_param
        sql_in = "SELECT field1, field2 FROM table1 WHERE field3 = ?"
        params = [ 1.to_i ]
        
        sql_out = FluidDb::Base.new.format_to_sql( sql_in, params )
        
        assert_equal "SELECT field1, field2 FROM table1 WHERE field3 = 1", sql_out
    end
    
    def test_single_string_param_no_quotes
        sql_in = "SELECT field1, field2 FROM table1 WHERE field3 = ?"
        params = [ 1.to_s ]
        
        sql_out = FluidDb::Base.new.format_to_sql( sql_in, params )
        
        assert_equal "SELECT field1, field2 FROM table1 WHERE field3 = '1'", sql_out
    end
    
    def test_single_string_param_with_quotes
        sql_in = "SELECT field1, field2 FROM table1 WHERE field3 = ?"
        params = [ "1'2".to_s ]
        
        sql_out = FluidDb::Base.new.format_to_sql( sql_in, params )
        
        assert_equal "SELECT field1, field2 FROM table1 WHERE field3 = '1\'2'", sql_out
    end
    
    def test_single_date_param
        sql_in = "SELECT field1, field2 FROM table1 WHERE field3 = ?"
        params = [ Date.parse( "1 jun 2012" ) ]
        
        sql_out = FluidDb::Base.new.format_to_sql( sql_in, params )
        
        assert_equal "SELECT field1, field2 FROM table1 WHERE field3 = '2012-06-01'", sql_out
    end
    
    def test_single_datetime_param
        sql_in = "SELECT field1, field2 FROM table1 WHERE field3 = ?"
        params = [ DateTime.parse( "1 jun 2012 11:15:00" ) ]
        
        sql_out = FluidDb::Base.new.format_to_sql( sql_in, params )
        
        assert_equal "SELECT field1, field2 FROM table1 WHERE field3 = '2012-06-01 11:15:00'", sql_out
    end
    
    def test_multiple_params_with_quotes
        sql_in = "SELECT field1, field2 FROM table1 WHERE field3 = ? AND field4 = ? OR field5 = ?"
        params = [ "1'2".to_s, 1.to_i, 1.4 ]
        
        sql_out = FluidDb::Base.new.format_to_sql( sql_in, params )
        
        assert_equal "SELECT field1, field2 FROM table1 WHERE field3 = '1\'2' AND field4 = 1 OR field5 = 1.4", sql_out
    end
    
    def test_insert_statement
        sql_in = "UPDATE table1 SET field1=?, field2=? WHERE field3=?"
        params = [ 1.to_f, 2.to_s, 3.to_i ]
        
        sql_out = FluidDb::Base.new.format_to_sql( sql_in, params )
        
        assert_equal "UPDATE table1 SET field1=1.0, field2='2' WHERE field3=3", sql_out
    end
end
