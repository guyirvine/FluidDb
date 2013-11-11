require "FluidDb"
require "mysql"

module FluidDb
    
    class Mysql<Base
        
        # Connect to Db.
        #
        # @param [String] uri a location for the resource to which we will attach, eg mysql://user:pass@127.0.0.1/foo
        def connect()
            uri = @uri
            database = uri.path.sub( "/", "" )
            
            @connection = ::Mysql.new uri.host, uri.user, uri.password, database, nil, nil, ::Mysql::CLIENT_FOUND_ROWS
        end
        
        def close
            @connection.close
        end

        def queryForArray( sql, params=[] )
            sql = self.format_to_sql( sql, params )
            results = @connection.query(sql)
            
            #        if ( $result === false ) then
            #    $message = pg_last_error( $this->connection );
            #    throw new Fluid_ConnectionException( $message );
            #end
            
            case results.num_rows
                when -1
                raise FluidDb::ConnectionError.new
                when 0
                raise FluidDb::NoDataFoundError.new
                when 1
                r = results.fetch_hash
                return r
                else
                raise FluidDb::TooManyRowsError.new
            end
            
        end
        
        def queryForValue( sql, params=[] )
            sql = self.format_to_sql( sql, params )
            results = @connection.query(sql)
            
            #        if ( $result === false ) then
            #    $message = pg_last_error( $this->connection );
            #    throw new Fluid_ConnectionException( $message );
            #end
            
            case results.num_rows
                when -1
                raise FluidDb::ConnectionError.new
                when 0
                raise FluidDb::NoDataFoundError.new
                when 1
                r=nil;
                results.each do |row|
                    r=row
                end
                return r[0]
                else
                raise FluidDb::TooManyRowsError.new
            end
            
        end
        
        def queryForResultset( sql, params=[] )
            sql = self.format_to_sql( sql, params )
            results = @connection.query(sql)
            
            #        if ( $result === false ) then
            #    $message = pg_last_error( $this->connection );
            #    throw new Fluid_ConnectionException( $message );
            #end
            
            case results.num_rows
                when -1
                raise FluidDb::ConnectionError.new
                else
                list = Array.new
                results.each_hash do |row|
                    list.push row
                end
                
                return list
            end
        end
        
        def execute( sql, params=[], expected_affected_rows=nil )
            sql = self.format_to_sql( sql, params )
            #            puts "sql: #{sql}"
            @connection.query( sql );
            
            if !expected_affected_rows.nil? and
                @connection.affected_rows != expected_affected_rows then
                raise ExpectedAffectedRowsError.new( "Expected affected rows, #{expected_affected_rows}, Actual affected rows, #{@connection.affected_rows}")
            end
        end
        
        def insert( sql, params )
            self.execute( sql, params )
            return @connection.insert_id
        end
        
    end
    
end
