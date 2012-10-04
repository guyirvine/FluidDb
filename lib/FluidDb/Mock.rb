require "FluidDb"

module FluidDb
    
    #A constant way of enabling testing for FluidDb
    class Mock<Base
        
        def initialize
            @hash = Hash.new
        end
        
        def connect()
        end
        
        def close
        end
        
        def queryForArray( sql, params )
            sql = self.format_to_sql( sql, params )
            return @hash[sql]
        end
        
        def queryForValue( sql, params )
            sql = self.format_to_sql( sql, params )
            return @hash[sql]
        end
        
        def queryForResultset( sql, params )
            sql = self.format_to_sql( sql, params )
            return @hash[sql]
        end
        
        def execute( sql, params, expected_affected_rows=nil )
            sql = self.format_to_sql( sql, params )
            return @hash[sql]
        end
        
        def insert( sql, params )
            raise "Pgsql uses SEQUENCES, so possibly easier to use 2 executes"
        end
        
        def addSql( sql, result )
            @hash[sql] = result;
        end

        def addSqlWithParams( sql, params, result )
            sql = self.format_to_sql( sql, params )
            
            self.addSql( sql, result )
        end
        
    end
    
end
