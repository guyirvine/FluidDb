require "FluidDb"
require "mysql2"

module FluidDb

    class Mysql<Base
        
        def initialize(uri)
            host = uri.host
            database = uri.path.sub( "/", "" )
            
            @connection = Mysql2::Client.new(:host => uri.host,
                                             :database => uri.path.sub( "/", "" ),
                                             :username => uri.user )
        end
        
        def queryForArray( sql, params )
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

        def queryForValue( sql, params )
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
        
        
        def queryForResultset( sql, params )
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
                return results
            end
        end
        
        
        #    def execute( sql, params, expected_affected_rows )
        def execute( sql, params )
            sql = self.format_to_sql( sql, params )
            @connection.query( sql );
        end
        
        def insert( sql, params )
            self.execute( sql, params )
            return @connection.last_id
        end
        
    end
    
end
