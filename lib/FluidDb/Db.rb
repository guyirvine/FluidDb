require "FluidDb"

module FluidDb
    
    def FluidDb.Db( uri )
	uri = URI.parse( uri ) if uri.is_a? String
        
        case uri.scheme
            when "mysql"
            require "FluidDb/Mysql"
            return FluidDb::Mysql.new( uri )
            when "mysql2"
            require "FluidDb/Mysql2"
            return FluidDb::Mysql2.new( uri )
            when "pgsql"
            require "FluidDb/Pgsql"
            return FluidDb::Pgsql.new( uri )
            when "fb"
            require "FluidDb/Firebird"
            return FluidDb::Firebird.new( uri )
            when "mock"
            require "FluidDb/Mock"
            return FluidDb::Mock.new( uri )
            when "tinytds"
            require "FluidDb/TinyTds"
            return FluidDb::TinyTds.new( uri )

            else
            abort("Scheme, #{uri.scheme}, not recognised when configuring creating db connection");
        end
        
    end
end
