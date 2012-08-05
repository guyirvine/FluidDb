require 'date'
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
    
    class Base
        
        @connection;

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
                    elsif v.kind_of? Date then
                    v = "'" + v.to_s + "'"
                    sql = sql.sub( "?", v.to_s )
                    elsif v.is_a?(Numeric) then
                    sql = sql.sub( "?", v.to_s )
                    else
                    raise FluidDb_ParamTypeNotSupportedError.new
                end
            end
            
            return sql
        end
        
        def queryForArray( sql, params )
            raise NotImplementedError.new("You must implement 'queryForArray'.")
        end
        
        def queryForValue( sql, params )
            raise NotImplementedError.new("You must implement 'queryForValue'.")
        end
        
        def queryForResultset( sql, params )
            raise NotImplementedError.new("You must implement 'queryForResultset'.")
        end
        
        def execute( sql, params, expected_affected_rows )
            raise NotImplementedError.new("You must implement 'execute'.")
        end
        
    end
    
end
