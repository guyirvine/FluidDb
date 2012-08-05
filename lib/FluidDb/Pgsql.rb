require "FluidDb"
require "pg"

module FluidDb
    
    class Pgsql<Base
        
        def initialize(uri)
            host = uri.host
            database = uri.path.sub( "/", "" )
            
            @connection = PG.connect( dbname:uri.path.sub( "/", "" ) )
        end
        
        def convertTupleToHash( fields, tuple, j )
            hash = Hash.new
            0.upto( fields.length-1 ).each do |i|
                hash[fields[i].to_s] = tuple.getvalue(j, i)
            end
            
            return hash
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
                results = @connection.query(sql, :as => :array)
                
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
                results = @connection.query(sql)
                
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
                    0.upto( results.num_tuples -1 ) do |nbr|
                        list.push self.convertTupleToHash(fields, results, nbr)
                    end
                    
                    return list
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
