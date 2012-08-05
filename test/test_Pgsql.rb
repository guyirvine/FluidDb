require 'test/unit'
require './lib/FluidDb/Pgsql'


class PsqlSQLTest < Test::Unit::TestCase

    def setup
        @FluidDb = FluidDb::Pgsql.new( URI.parse( "pgsql://localhost/test" ) )
        @FluidDb.execute( "DROP TABLE table1", [])
        @FluidDb.execute( "CREATE TABLE table1 ( field1 BIGINT, field2 VARCHAR(50) );", [])
        
        @FluidDb.execute( "INSERT INTO table1 ( field1, field2 ) VALUES ( 1, 'Two' );", [])
        @FluidDb.execute( "INSERT INTO table1 ( field1, field2 ) VALUES ( 2, 'Three' );", [])
    end

    def test_queryForArray
        sql_in = "SELECT field1, field2 FROM table1 WHERE field1 = 1"

        r = @FluidDb.queryForArray( sql_in, [] )
        
        assert_equal "{\"field1\"=>\"1\", \"field2\"=>\"Two\"}", r.to_s
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
        
        assert_equal "[{\"field1\"=>\"1\", \"field2\"=>\"Two\"}, {\"field1\"=>\"2\", \"field2\"=>\"Three\"}]", resultset.to_s
        
    end

    def test_delete
        @FluidDb.execute( "DELETE FROM table1 WHERE field1 = ?", [1])
        count = @FluidDb.queryForValue( "SELECT count(*) FROM table1 WHERE field1 > ?", [0] )
        
        assert_equal 1, count.to_i
        
    end

end
