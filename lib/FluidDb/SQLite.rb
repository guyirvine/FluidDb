require "FluidDb"
require "sqlite3"

module FluidDb

    class SQLite<Base

        # Connect to Db.
        #
        # @param [String] uri a location for the resource to which we will attach, eg mysql://user:pass@127.0.0.1/foo
        def connect()
            uri = @uri
            @connection = SQLite3::Database.new uri.path
        end

        def close
            @connection.close
        end

        def queryForArray( sql, params=[] )
            sql = self.format_to_sql( sql, params )
            @connection.results_as_hash = true
            results = @connection.execute(sql)

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
                return results[0]
                else
                raise FluidDb::TooManyRowsError.new
            end
        end

        def queryForValue( sql, params=[] )
            sql = self.format_to_sql( sql, params )
            @connection.results_as_hash = false
            results = @connection.execute(sql)

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
                else
                raise FluidDb::TooManyRowsError.new
            end

        end


        def queryForResultset( sql, params=[] )
            sql = self.format_to_sql( sql, params )
            @connection.results_as_hash = true
            results = @connection.execute(sql)

            #        if ( $result === false ) then
            #    $message = pg_last_error( $this->connection );
            #    throw new Fluid_ConnectionException( $message );
            #end

            case results.length
                when -1
                raise FluidDb::ConnectionError.new
                else
                return results
            end
        end


        def execute( sql, params=[], expected_affected_rows=nil )
            sql = self.format_to_sql( sql, params )

            self.verboseLog( "#{self.class.name}.execute. #{sql}" )
            r = @connection.execute(sql)

            if !expected_affected_rows.nil? and
                r.changes != expected_affected_rows then
                raise ExpectedAffectedRowsError.new( "Expected affected rows, #{expected_affected_rows}, Actual affected rows, #{r.cmd_tuples}")
            end
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
                r.changes != expected_affected_rows then
                raise ExpectedAffectedRowsError.new( "Expected affected rows, #{expected_affected_rows}, Actual affected rows, #{r.cmd_tuples}")
            end
        end

        # Transaction Semantics
        def Begin
            @connection.transaction
        end

        # Transaction Semantics
        def Commit
            @connection.commit
        end

        # Transaction Semantics
        def Rollback
            @connection.rollback
        end

        def insert( sql, params )
            raise "SQLite uses SEQUENCES, so possibly easier to use 2 executes"
            #            self.execute( sql, params )
            #return @connection.last_id
        end

    end

end
