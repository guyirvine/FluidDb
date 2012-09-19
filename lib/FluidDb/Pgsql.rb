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
            database = uri.path.sub( "/", "" )
            
            @connection = PG.connect( dbname:uri.path.sub( "/", "" ) )
        end

        def close
            begin
                @connection.close
                rescue
                puts "FluidDb::Pgsql. An error was raised while closing connection to, " + uri.to_s
            end
        end
        
        def queryForArray( sql, params )
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
        
        def queryForValue( sql, params )
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
        
        
        def queryForResultset( sql, params )
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
        

        def execute( sql, params, expected_affected_rows=nil )
            sql = self.format_to_sql( sql, params )
            r = @connection.query( sql );
            
            if !expected_affected_rows.nil? and
                r.cmd_tuples != expected_affected_rows then
                raise ExpectedAffectedRowsError.new( "Expected affected rows, #{expected_affected_rows}, Actual affected rows, #{r.cmd_tuples}")
            end
        end

        def insert( sql, params )
            raise "Pgsql uses SEQUENCES, so possibly easier to use 2 executes"
            #            self.execute( sql, params )
            #return @connection.last_id
        end

    end
    
end
