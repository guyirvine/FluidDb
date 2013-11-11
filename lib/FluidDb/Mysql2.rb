require "FluidDb"
require "mysql2"

module FluidDb
    
    class Mysql2<Base
        
        # Connect to Db.
        #
        # @param [String] uri a location for the resource to which we will attach, eg mysql://user:pass@127.0.0.1/foo
        def connect()
            uri = @uri
            host = uri.host
            database = uri.path.sub( "/", "" )
            
            
            @connection = ::Mysql2::Client.new(:host => uri.host,
                                               :database => uri.path.sub( "/", "" ),
                                               :username => uri.user,
                                               :flags => ::Mysql2::Client::FOUND_ROWS )
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
            
            case results.count
                when -1
                raise FluidDb::ConnectionError.new
                when 0
                raise FluidDb::NoDataFoundError.new
                when 1
                r=nil;
                results.each do |row|
                    r=row
                end
                return r
                else
                raise FluidDb::TooManyRowsError.new
            end
            
        end
        
        def queryForValue( sql, params=[] )
            sql = self.format_to_sql( sql, params )
            results = @connection.query(sql, :as => :array)
            
            #        if ( $result === false ) then
            #    $message = pg_last_error( $this->connection );
            #    throw new Fluid_ConnectionException( $message );
            #end
            
            case results.count
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
            
            case results.count
                when -1
                raise FluidDb::ConnectionError.new
                else
                list = Array.new
                results.each do |row|
                    list.push row
                end
                
                return list
            end
        end
        
        def execute( sql, params=[], expected_affected_rows=nil )
            sql = self.format_to_sql( sql, params )
            @connection.query( sql );
            
            if !expected_affected_rows.nil? and
                @connection.affected_rows != expected_affected_rows then
                raise ExpectedAffectedRowsError.new( "Expected affected rows, #{expected_affected_rows}, Actual affected rows, #{@connection.affected_rows}")
            end
        end
        
        
        def insert( sql, params )
            self.execute( sql, params )
            return @connection.last_id
        end
        
    end
    
end
