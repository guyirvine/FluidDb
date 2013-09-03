require "FluidDb"
require "tiny_tds"
require "cgi"

module FluidDb
    
    class TinyTds<Base
        
        # Connect to Db.
        #
        # @param [String] uri a location for the resource to which we will attach, eg tinytds://<user>:<pass>@<dataserver>/<database>
        def connect()
            uri = @uri
            
            dataserver = uri.host
            database = uri.path.sub( "/", "" )
            username = URI.unescape( uri.user )
            password = uri.password

            
            if dataserver == "" ||
                database == "" then
                raise "*** You need to specify both a dataserver and a database for the tinytds driver. Expected format: tinytds://<user>:<pass>@<dataserver>/<database>\n" +
                "*** The specified dataserver should have an entry in /etc/freetds/freetds.conf"
            end
            
            if username == "" ||
                password == "" then
                puts "*** Warning - you will normally need to specify both a username and password for the tinytds driver to work correctly."
            end
            
            hash = Hash[:username, username, :password, password, :database, database, :dataserver, dataserver]
            if !uri.query.nil? then
                cgi = CGI.parse( uri.query )
                hash[:timeout] = cgi["timeout"][0].to_i if cgi.has_key?( "timeout" )
            end

            @connection = ::TinyTds::Client.new( hash )            
            
            if !@connection.active? then
                raise "Unable to connect to the database"
            end
        end
        
        def close
            @connection.close
        end
        
        def escape_string( input )
            return @connection.escape( input )
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
