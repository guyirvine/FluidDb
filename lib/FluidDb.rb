require 'date'

class FluidDb_ConnectionError<StandardError
end
class FluidDb_NoDataFoundError<StandardError
end
class FluidDb_TooManyRowsError<StandardError
end
class FluidDb_ParamTypeNotSupportedError<StandardError
end

class FluidDb
    
    @connection;
    
    def format_to_sql( sql, params=nil )
        if params.nil? then
            return sql
        end
        
        params.each do |v|
            if v.kind_of? String then
                v = "'" + v.sub( "'", "\'" ) + "'"
                sql = sql.sub( "?", v )
                elsif v.kind_of? Date then
                v = "'" + v.to_s + "'"
                sql = sql.sub( "?", v.to_s )
                elsif v.kind_of? DateTime then
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
