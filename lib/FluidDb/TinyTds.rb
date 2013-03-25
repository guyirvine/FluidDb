require "FluidDb"
require "tiny_tds"

module FluidDb
    
    class TinyTds<Base
        
        # Connect to Db.
        #
        # @param [String] uri a location for the resource to which we will attach, eg tinytds://user:pass@127.0.0.1
        def connect()
            uri = @uri
            raise "Unsupported uri. Please update freetds.conf and use format tinytds://<user>:<pass>@<dataserver>" unless uri.path == ""
            
            dataserver = uri.host
            username = URI.unescape( uri.user )
            password = uri.password
            
            puts "#{username}, #{password}, #{dataserver}"
            
            @connection = ::TinyTds::Client.new( :username => username, :password => password, :dataserver => dataserver )
        end

        def close
            @connection.close
        end
        
        def queryForArray( sql, params )
            sql = self.format_to_sql( sql, params )
            results = @connection.execute(sql)

            count = 0
            tuple = ""
            results.each do |row|
                count = count + 1
                raise FluidDb::TooManyRowsError.new if count > 1
                
                tuple = row
            end
            
            raise FluidDb::NoDataFoundError.new if count == 0
            
            return tuple
        end
        
        def queryForValue( sql, params )
            sql = self.format_to_sql( sql, params )
            results = @connection.execute(sql)
            
            count = 0
            value = ""
            results.each do |row|
                count = count + 1
                raise FluidDb::TooManyRowsError.new if count > 1
                
                value = row[results.fields[0]]
            end
            
            raise FluidDb::NoDataFoundError.new if count == 0
            
            return value
        end
        
        
        def queryForResultset( sql, params )
            sql = self.format_to_sql( sql, params )
            results = @connection.execute(sql)
            
            list = Array.new
            results.each do |row|
                list << row
            end

            return list
        end
        
        
        def execute( sql, params, expected_affected_rows=nil )
            sql = self.format_to_sql( sql, params )
            r = @connection.execute( sql );
            r.each

            if !expected_affected_rows.nil? and
                r.affected_rows != expected_affected_rows then
                raise ExpectedAffectedRowsError.new( "Expected affected rows, #{expected_affected_rows}, Actual affected rows, #{r.affected_rows}")
            end
        end

        def insert( sql, params )
            raise "Pgsql uses SEQUENCES, so possibly easier to use 2 executes"
            #            self.execute( sql, params )
            #return @connection.last_id
        end
        
    end
    
end
