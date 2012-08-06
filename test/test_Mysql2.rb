require 'test/unit'
require './lib/FluidDb/Mysql2'


class Mysql2SQLTest < Test::Unit::TestCase
    
    def setup
        @FluidDb = FluidDb::Mysql2.new( URI.parse( "mysql2://localhost/test" ) )
        @FluidDb.execute( "DROP TABLE table1", [])
        @FluidDb.execute( "CREATE TABLE table1 ( id BIGINT NOT NULL AUTO_INCREMENT, field1 BIGINT, field2 VARCHAR(50), PRIMARY KEY (id) );", [])
        
        @FluidDb.execute( "INSERT INTO table1 ( field1, field2 ) VALUES ( 1, 'Two' );", [])
        @FluidDb.execute( "INSERT INTO table1 ( field1, field2 ) VALUES ( 2, 'Three' );", [])
    end
    
    def test_queryForArray
        sql_in = "SELECT field1, field2 FROM table1 WHERE field1 = 1"
        
        r = @FluidDb.queryForArray( sql_in, [] )
        
        assert_equal "{\"field1\"=>1, \"field2\"=>\"Two\"}", r.to_s
    end
    
    def test_queryForArrayTooManyRows
        error_raised = false
        sql_in = "SELECT field1, field2 FROM table1"
        
        begin
            r = @FluidDb.queryForArray( sql_in, [] )
            rescue FluidDb::TooManyRowsError
            error_raised = true
        end
        
        assert_equal true, error_raised
    end
    
    def test_queryForValue
        sql_in = "SELECT field2 FROM table1 WHERE field1 = 1"
        
        field1 = @FluidDb.queryForValue( sql_in, [] )
        
        assert_equal "Two", field1
    end
    
    def test_queryForValueNoDataFound
        error_raised = false
        sql_in = "SELECT field1, field2 FROM table1 WHERE field1 = ?"
        
        begin
            r = @FluidDb.queryForValue( sql_in, [-1] )
            rescue FluidDb::NoDataFoundError
            error_raised = true
        end
        
        assert_equal true, error_raised
    end
    
    def test_queryForResultset
        sql_in = "SELECT field1, field2 FROM table1 WHERE field1 > ?"
        
        resultset = @FluidDb.queryForResultset( sql_in, [0] )
        
        assert_equal "[{\"field1\"=>1, \"field2\"=>\"Two\"}, {\"field1\"=>2, \"field2\"=>\"Three\"}]", resultset.to_s
        
    end
    
    def test_delete
        @FluidDb.execute( "DELETE FROM table1 WHERE field1 = ?", [1])
        count = @FluidDb.queryForValue( "SELECT count(*) FROM table1 WHERE field1 > ?", [0] )
        
        assert_equal 1, count.to_i
        
    end
    
    def test_update_without_expected_affected_rows
        @FluidDb.execute( "UPDATE table1 SET field2 = ? WHERE field1 = ?", ["One", 1])
        
        field2 = @FluidDb.queryForValue( "SELECT field2 FROM table1 WHERE field1 = ?", [1] )
        assert_equal "One", field2.to_s
    end
    
    def test_update_with_correct_expected_affected_rows
        @FluidDb.execute( "UPDATE table1 SET field2 = ? WHERE field1 = ?", ["One", 1], 1)
        
        field2 = @FluidDb.queryForValue( "SELECT field2 FROM table1 WHERE field1 = ?", [1] )
        assert_equal "One", field2.to_s
    end
    
    def test_update_with_incorrect_expected_affected_rows
        error_raised = false
        begin
            @FluidDb.execute( "UPDATE table1 SET field2 = ? WHERE field1 = ?", ["One", 1], 2)
            rescue FluidDb::ExpectedAffectedRowsError
            error_raised = true
        end
        
        field2 = @FluidDb.queryForValue( "SELECT field2 FROM table1 WHERE field1 = ?", [1] )
        assert_equal "One", field2.to_s
        assert_equal true, error_raised
        
    end
    
    def test_update_with_correct_expected_matched_rows
        @FluidDb.execute( "UPDATE table1 SET field2 = ? WHERE field1 = ?", ["Two", 1], 1)
        
        field2 = @FluidDb.queryForValue( "SELECT field2 FROM table1 WHERE field1 = ?", [1] )
        assert_equal "Two", field2.to_s
    end
    
    def test_update_with_incorrect_expected_matched_rows
        error_raised = false
        begin
            @FluidDb.execute( "UPDATE table1 SET field2 = ? WHERE field1 = ?", ["Two", 1], 2)
            rescue FluidDb::ExpectedAffectedRowsError
            error_raised = true
        end
        
        field2 = @FluidDb.queryForValue( "SELECT field2 FROM table1 WHERE field1 = ?", [1] )
        assert_equal "Two", field2.to_s
        assert_equal true, error_raised
        
    end
        
    def test_insert
        id = @FluidDb.insert( "INSERT INTO table1 ( field1, field2 ) VALUES ( ?, ? );", [3, 'Four'] )
        
    end

end
