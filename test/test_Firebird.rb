require 'test/unit'
require './lib/FluidDb/Firebird'


class FirebirdSQLTest < Test::Unit::TestCase

    def setup
        #        File.delete( "/tmp/test.fb" ) if File.exists?( "/tmp/test.fb" )
        @FluidDb = FluidDb::Firebird.new( URI.parse( "fb://localhost/tmp/test.fb" ) )
        #                begin
        #            @FluidDb.execute( "DROP TABLE table1", [])
        #rescue Exception => e
        #            raise e if e.message.index( "Table TABLE1 does not exist" ).nil?
        #end
        begin
        @FluidDb.execute( "CREATE TABLE table1 ( field1 BIGINT, field2 VARCHAR(50) );", [])
        rescue
        end
        @FluidDb.execute( "DELETE FROM table1;", [])
        @FluidDb.execute( "INSERT INTO table1 ( field1, field2 ) VALUES ( 1, 'Two' );", [])
        @FluidDb.execute( "INSERT INTO table1 ( field1, field2 ) VALUES ( 2, 'Three' );", [])
    end

    def test_queryForArray
        sql_in = "SELECT field1, field2 FROM table1 WHERE field1 = 1"

        r = @FluidDb.queryForArray( sql_in, [] )
        
        assert_equal "{\"FIELD1\"=>1, \"FIELD2\"=>\"Two\"}", r.to_s
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
        
        assert_equal "[{\"FIELD1\"=>1, \"FIELD2\"=>\"Two\"}, {\"FIELD1\"=>2, \"FIELD2\"=>\"Three\"}]", resultset.to_s
        
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
    
    
end
