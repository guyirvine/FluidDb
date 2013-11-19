require "FluidDb"

module FluidDb
    
    #A constant way of enabling testing for FluidDb
    class Mock<Base
        
        @verbose = false
        
        def initialize
            @hash = Hash.new
            
        end
        
        def verbose()
            @verbose = true
            return self
        end
        
        def connect()
        end
        
        def close
        end
        
        def queryForArray( sql, params=[] )
            sql = self.format_to_sql( sql, params )
            puts "FluidDb::Mock.queryForArray. sql: #{sql}" if @verbose == true
            
            results = @hash[sql]
            case results.length
                when 0
                raise FluidDb::NoDataFoundError.new
                when 1
                return results.first
                return r
                else
                raise FluidDb::TooManyRowsError.new
            end
            
        end
        
        def queryForValue( sql, params=[] )
            sql = self.format_to_sql( sql, params )
            puts "FluidDb::Mock.queryForValue. sql: #{sql}" if @verbose == true
            
            results = @hash[sql]
            case results.length
                when 0
                raise FluidDb::NoDataFoundError.new
                when 1
                return results.first.first[1]
                else
                raise FluidDb::TooManyRowsError.new
            end
            return @hash[sql]
        end
        
        def queryForResultset( sql, params=[] )
            sql = self.format_to_sql( sql, params )
            puts "FluidDb::Mock.queryForResultset. sql: #{sql}" if @verbose == true
            return @hash[sql]
        end
        
        def execute( sql, params=[], expected_affected_rows=nil )
            sql = self.format_to_sql( sql, params )
            puts "FluidDb::Mock.execute. sql: #{sql}" if @verbose == true
            return @hash[sql]
        end
        
        def insert( sql, params )
            raise "Pgsql uses SEQUENCES, so possibly easier to use 2 executes"
        end
        
        def addSql( sql, result )
            if !result.is_a? Array then
                raise TypeError.new( "Expecting an Array of Hashes, eg [{'field1'=>1, 'field2'=>2}]. Note, the Array may be empty" )
            end
            
            @hash[sql] = result;
        end
        
        def addSqlWithParams( sql, params, result )
            sql = self.format_to_sql( sql, params )
            
            self.addSql( sql, result )
        end
        
    end
    
end
