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
    
end
