require 'date'
require 'time'
require 'uri'

require "FluidDb/Db"

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
    class IncorrectNumberOfParametersError<StandardError
    end
    class DuplicateKeyError<StandardError
    end

    class Base
        
        attr_writer :verbose
        attr_reader :connection
        
        @connection;
        @uri
        @verbose
        
        # Constructor.
        #
        # @param [String] uri a location for the resource to which we will attach, eg mysql://user:pass@127.0.0.1/foo
        def initialize(uri)
            if uri.kind_of? String then
                @uri = URI.parse( uri )
                else
                @uri = uri
            end
            
            self.connect
            
            @verbose = !ENV["VERBOSE"].nil?
        end
        
        def verboseLog( string )
            puts string if @verbose == true
        end

        def splice_sql( sql, params )
            
            if params.length != sql.count( "?" ) then
                raise IncorrectNumberOfParametersError.new
            end

            sql_out = ""
            sql.split( "?" ).each_with_index do |s,idx|
                sql_out = sql_out + s
                sql_out = sql_out + params[idx] unless params[idx].nil?
            end
            
            return sql_out
        end
        
        def escape_string( input )
            return input.split( "'" ).join( "''" )
        end
        
        def format_to_sql( sql, params=nil )
            if params.nil? || params.count == 0 then
                return sql
            end
            
            params.each_with_index do |v, idx|
                if v.kind_of? String then
                    v = "'" + self.escape_string( v ) + "'"
                    #v = "'" + v.sub( "'", "\'" ) + "'"
                elsif v.is_a? DateTime then
                    v = "'" + v.strftime( "%Y-%m-%d %H:%M:%S.%6N %z" ) + "'"
                elsif v.is_a? Time then
                    v = "'" + v.strftime( "%Y-%m-%d %H:%M:%S.%6N %z" ) + "'"
                elsif v.kind_of? Date then
                    v = "'" + v.to_s + "'"
                elsif v.is_a? Numeric then
                    v = v.to_s
                elsif v.is_a? TrueClass then
                    v = "true"
                elsif v.is_a? FalseClass then
                    v = "false"
                elsif v.nil? then
                    v = 'NULL'
                else
                    raise ParamTypeNotSupportedError.new( "Name of unknown param type, #{v.class.name}, for sql, #{sql}" )
                end
                params[idx] = v
            end
            
            sql_out = self.splice_sql( sql, params )
            if @verbose == true then
                puts self.class.name
                puts sql
                puts params.join(",")
                puts sql_out
                #puts "#{self.class.name}\n#{sql}\n#{params.join(',')\n#{sql_out}}"
            end
            
            return sql_out
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
        # Throws an error for no data.
        # Throws an error for more than 1 row
        #
        # @param [String] sql The SELECT statement to run
        # @param [Array] parama The parameters to be added to the sql query. Ruby types are used to determine formatting and escaping.
        def queryForArray( sql, params )
            raise NotImplementedError.new("You must implement 'queryForArray'.")
        end
        
        # Return a single value is returned from a single row from the database, given the sql parameter.
        # Throws an error for no data.
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

        # Transaction Semantics
        def Begin
            @connection.execute( "BEGIN", [] )
        end

        # Transaction Semantics
        def Commit
            @connection.execute( "COMMIT", [] )
        end

        # Transaction Semantics
        def Rollback
            @connection.execute( "ROLLBACK", [] )
        end
        
    end
    
end
