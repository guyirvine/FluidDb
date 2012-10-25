require 'date'
require 'time'
require 'uri'

module FluidDb
    
    class ConnectionError<StandardError
    end
    class NoDataFoundError<StandardError
    end
    class TooManyRowsError<StandardError
    end
    class ParamTypeNotSupportedError<StandardError
    end
    class ExpectedAffectedRowsError<StandardError
    end
    
    class Base
        
        @connection;
        @uri

        # Constructor.
        #
        # @param [String] uri a location for the resource to which we will attach, eg mysql://user:pass@127.0.0.1/foo
        def initialize(uri)
            if uri.kind_of? String then
                @uri = Uri.parse( uri )
            else
                @uri = uri
            end

            self.connect
        end

        def format_to_sql( sql, params=nil )
            if params.nil? then
                return sql
            end
            #timestamp.strftime( "%Y-%m-%d %H:%M:%S" )
            params.each do |v|
                if v.kind_of? String then
                    v = "'" + v.sub( "'", "\'" ) + "'"
                    sql = sql.sub( "?", v )
                    elsif v.is_a? DateTime then
                    s = "'" + v.strftime( "%Y-%m-%d %H:%M:%S" ) + "'"
                    sql = sql.sub( "?", s )
                    elsif v.is_a? Time then
                    s = "'" + v.strftime( "%Y-%m-%d %H:%M:%S" ) + "'"
                    sql = sql.sub( "?", s )
                    elsif v.kind_of? Date then
                    v = "'" + v.to_s + "'"
                    sql = sql.sub( "?", v.to_s )
                    elsif v.is_a?(Numeric) then
                    sql = sql.sub( "?", v.to_s )
                    else
                    raise ParamTypeNotSupportedError.new( "Name of unknown param type, #{v.class.name}, for sql, #{sql}" )
                end
            end

            return sql
        end
        
        def convertTupleToHash( fields, tuple, j )
            hash = Hash.new
            0.upto( fields.length-1 ).each do |i|
                hash[fields[i].to_s] = tuple.getvalue(j, i)
            end
            
            return hash
        end
        
        def connect
            raise NotImplementedError.new("You must implement 'connect'.")
        end
        
        def close
            raise NotImplementedError.new("You must implement 'close'.")
        end

        def reconnect
            self.close
            self.connect
        end

        # Return a single row from the database, given the sql parameter.
        # Throwa an error for no data.
        # Throws an error for more than 1 row
        #
        # @param [String] sql The SELECT statement to run
        # @param [Array] parama The parameters to be added to the sql query. Ruby types are used to determine formatting and escaping.
        def queryForArray( sql, params )
            raise NotImplementedError.new("You must implement 'queryForArray'.")
        end
        
        # Return a single value is returned from a single row from the database, given the sql parameter.
        # Throwa an error for no data.
        # Throws an error for more than 1 row
        #
        # @param [String] sql The SELECT statement to run
        # @param [Array] parama The parameters to be added to the sql query. Ruby types are used to determine formatting and escaping.
        def queryForValue( sql, params )
            raise NotImplementedError.new("You must implement 'queryForValue'.")
        end
        
        def queryForResultset( sql, params )
            raise NotImplementedError.new("You must implement 'queryForResultset'.")
        end
        
        # Execute an insert, update or delete, then check the impact that statement has on the data.
        #
        # @param [String] sql The SELECT statement to run
        # @param [Array] parama The parameters to be added to the sql query. Ruby types are used to determine formatting and escaping.
        # @param [String] expected_affected_rows The number of rows that should have been updated.
        def execute( sql, params, expected_affected_rows )
            raise NotImplementedError.new("You must implement 'execute'.")
        end
        
    end
    
end
