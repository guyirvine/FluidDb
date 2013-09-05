require "FluidDb"
require "fb"
include Fb

module FluidDb
    
    class Firebird<Base

        # Connect to Db.
        #
        # @param [String] uri a location for the resource to which we will attach, eg mysql://user:pass@127.0.0.1/foo
        def connect()
            uri = @uri
            
            user = uri.user || "sysdba"
            password = uri.password || "masterkey"
            port = uri.port || 3050

            path = uri.path
            path = path.slice(1,uri.path.length-1) if path.slice(0,3) == "/C:"
            path = URI.unescape( path )

            
            # The Database class acts as a factory for Connections.
            # It can also create and drop databases.
            db = Database.new(
                              :database => "#{uri.host}/#{port}:#{path}",
                              :username => user,
                              :password => password)
            # :database is the only parameter without a default.
            # Let's connect to the database, creating it if it doesn't already exist.
            @connection = db.connect rescue db.create.connect

        end

        def close
            @connection.close
        end

        def queryForArray( sql, params )
            sql = self.format_to_sql( sql, params )
            list = @connection.query(:hash, sql)

            #        if ( $result === false ) then
            #    $message = pg_last_error( $this->connection );
            #    throw new Fluid_ConnectionException( $message );
            #end

            case list.length
                when -1
                raise FluidDb::ConnectionError.new
                when 0
                raise FluidDb::NoDataFoundError.new
                when 1
                return list[0]
                else
                raise FluidDb::TooManyRowsError.new
            end
        end
        
        def queryForValue( sql, params )
            sql = self.format_to_sql( sql, params )
            results = @connection.query(sql)
            
            #        if ( $result === false ) then
            #    $message = pg_last_error( $this->connection );
            #    throw new Fluid_ConnectionException( $message );
            #end
            case results.length
                when -1
                raise FluidDb::ConnectionError.new
                when 0
                raise FluidDb::NoDataFoundError.new
                when 1
                return results[0][0]
                #                return results.getvalue(0,0)
                else
                raise FluidDb::TooManyRowsError.new
            end
            
        end
        
        
        def queryForResultset( sql, params )
            sql = self.format_to_sql( sql, params )
            list = @connection.query(:hash, sql)
            
            #        if ( $result === false ) then
            #    $message = pg_last_error( $this->connection );
            #    throw new Fluid_ConnectionException( $message );
            #end
            
            case list.length
                when -1
                raise FluidDb::ConnectionError.new
                else
                
                return list
            end
        end
        
        
        def execute( sql, params, expected_affected_rows=nil )
            sql = self.format_to_sql( sql, params )
            
            self.verboseLog( "#{self.class.name}.execute. #{sql}" )
            affected_rows = @connection.execute(sql)

            if !expected_affected_rows.nil? and
                affected_rows != expected_affected_rows then
                raise ExpectedAffectedRowsError.new( "Expected affected rows, #{expected_affected_rows}, Actual affected rows, #{affected_rows}")
            end
#            rescue PG::Error => e
#                raise DuplicateKeyError.new( e.message ) unless e.message.index( "duplicate key value violates unique constraint" ).nil?
            
#                raise e
        end
        
        def exec_params( sql, params, expected_affected_rows=nil )
                        parts = sql.split( "?" )
            sql = ""
            parts.each_with_index do |p,idx|
                sql = sql + p;
                sql = sql + "$#{idx+1}" if idx < parts.length - 1
            end
            affected_rows = @connection.exec_params( sql, params );
            
            if !expected_affected_rows.nil? and
                affected_rows != expected_affected_rows then
                raise ExpectedAffectedRowsError.new( "Expected affected rows, #{expected_affected_rows}, Actual affected rows, #{affected_rows}")
            end
#            rescue PG::Error => e
#            raise DuplicateKeyError.new( e.message ) unless e.message.index( "duplicate key value violates unique constraint" ).nil?
            
#            raise e
        end
        
        def insert( sql, params )
            raise "Pgsql uses SEQUENCES, so possibly easier to use 2 executes"
            #            self.execute( sql, params )
            #return @connection.last_id
        end

        # Transaction Semantics
        def Begin
            @connection.transaction()
        end

        # Transaction Semantics
        def Commit
            @connection.commit()
        end

        # Transaction Semantics
        def Rollback
            @connection.rollback()
        end
        
        
    end
    
end
