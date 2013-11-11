require "FluidDb"
require "pg"

module FluidDb
    
    class Pgsql<Base
        
        # Connect to Db.
        #
        # @param [String] uri a location for the resource to which we will attach, eg mysql://user:pass@127.0.0.1/foo
        def connect()
            uri = @uri
            host = uri.host
            dbname = uri.path.sub( "/", "" )
            
            hash = Hash["host", host, "dbname", dbname]
            hash["port"] = uri.port unless uri.port.nil?
            hash["user"] = uri.user unless uri.user.nil?
            hash["password"] = uri.password unless uri.password.nil?

            @connection = PG.connect( hash )
        end

        def close
            @connection.close
        end

        def queryForArray( sql, params=[] )
            sql = self.format_to_sql( sql, params )
            results = @connection.exec(sql)

            #        if ( $result === false ) then
            #    $message = pg_last_error( $this->connection );
            #    throw new Fluid_ConnectionException( $message );
            #end

            case results.num_tuples
                when -1
                raise FluidDb::ConnectionError.new
                when 0
                raise FluidDb::NoDataFoundError.new
                when 1
                return self.convertTupleToHash(results.fields, results, 0)
                else
                raise FluidDb::TooManyRowsError.new
            end
        end
        
        def queryForValue( sql, params=[] )
            sql = self.format_to_sql( sql, params )
            results = @connection.exec(sql)
            
            #        if ( $result === false ) then
            #    $message = pg_last_error( $this->connection );
            #    throw new Fluid_ConnectionException( $message );
            #end
            
            case results.num_tuples
                when -1
                raise FluidDb::ConnectionError.new
                when 0
                raise FluidDb::NoDataFoundError.new
                when 1
                return results.getvalue(0,0)
                else
                raise FluidDb::TooManyRowsError.new
            end
            
        end
        
        
        def queryForResultset( sql, params=[] )
            sql = self.format_to_sql( sql, params )
            results = @connection.exec(sql)
            
            #        if ( $result === false ) then
            #    $message = pg_last_error( $this->connection );
            #    throw new Fluid_ConnectionException( $message );
            #end
            
            case results.num_tuples
                when -1
                raise FluidDb::ConnectionError.new
                else
                list = Array.new
                fields = results.fields
                0.upto( results.ntuples() -1 ) do |nbr|
                    list.push self.convertTupleToHash(fields, results, nbr)
                end
                
                return list
            end
        end
        
        
        def execute( sql, params=[], expected_affected_rows=nil )
            sql = self.format_to_sql( sql, params )
            
            self.verboseLog( "#{self.class.name}.execute. #{sql}" )
            r = @connection.exec(sql)

            if !expected_affected_rows.nil? and
                r.cmd_tuples != expected_affected_rows then
                raise ExpectedAffectedRowsError.new( "Expected affected rows, #{expected_affected_rows}, Actual affected rows, #{r.cmd_tuples}")
            end
            rescue PG::Error => e
                raise DuplicateKeyError.new( e.message ) unless e.message.index( "duplicate key value violates unique constraint" ).nil?
            
                raise e
        end
        
        def exec_params( sql, params=[], expected_affected_rows=nil )
                        parts = sql.split( "?" )
            sql = ""
            parts.each_with_index do |p,idx|
                sql = sql + p;
                sql = sql + "$#{idx+1}" if idx < parts.length - 1
            end
            r = @connection.exec_params( sql, params );
            
            if !expected_affected_rows.nil? and
                r.cmd_tuples != expected_affected_rows then
                raise ExpectedAffectedRowsError.new( "Expected affected rows, #{expected_affected_rows}, Actual affected rows, #{r.cmd_tuples}")
            end
            rescue PG::Error => e
            raise DuplicateKeyError.new( e.message ) unless e.message.index( "duplicate key value violates unique constraint" ).nil?
            
            raise e
        end
        
        def insert( sql, params )
            raise "Pgsql uses SEQUENCES, so possibly easier to use 2 executes"
            #            self.execute( sql, params )
            #return @connection.last_id
        end
        
    end
    
end
