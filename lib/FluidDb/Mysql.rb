require "FluidDb"
require "mysql2"

class FluidDb_Mysql<FluidDb
    
    def initialize(uri)
        host = uri.host
        database = uri.path.sub( "/", "" )
        
        @connection = Mysql2::Client.new(:host => uri.host,
                                         :database => uri.path.sub( "/", "" ),
                                         :username => uri.user )
    end

    def format_to_sql( sql, params )
        return final_sql
    end
    
	def queryForArray( sql, params )
        final_sql = self.format_to_sql( sql, params )
        results = @connection.query(sql)
        
        #        if ( $result === false ) then
        #    $message = pg_last_error( $this->connection );
        #    throw new Fluid_ConnectionException( $message );
        #end
        
        results.count == 0
        case results.count
            when -1
            raise FluidDb_ConnectionError.new
            when 0
            raise FluidDb_NoDataFoundError.new
            when 1
            r=nil;
            results.each do |row|
                r=row
            end
            return r
            else
            raise FluidDb_TooManyRowsError.new
        end
        
    end
    
    def queryForValue( sql, params )
        final_sql = self.format_to_sql( sql, params )
        results = @connection.query(sql, :as => :array)
        
        #        if ( $result === false ) then
        #    $message = pg_last_error( $this->connection );
        #    throw new Fluid_ConnectionException( $message );
        #end
        
        results.count == 0
        case results.count
            when -1
            raise FluidDb_ConnectionError.new
            when 0
            raise FluidDb_NoDataFoundError.new
            when 1
            r=nil;
            results.each do |row|
                r=row
            end
            return r[0]
            else
            raise FluidDb_TooManyRowsError.new
        end
        
    end
    
    
    def queryForResultset( sql, params )
        final_sql = self.format_to_sql( sql, params )
        results = @connection.query(sql)
        
        #        if ( $result === false ) then
        #    $message = pg_last_error( $this->connection );
        #    throw new Fluid_ConnectionException( $message );
        #end
        
        results.count == 0
        case results.count
            when -1
            raise FluidDb_ConnectionError.new
            else
            return results
        end
    end
    
    
    def execute( sql, params, expected_affected_rows )
        final_sql = self.format_to_sql( sql, params )
        @connection.query( sql, params );
    end
    
    
end
