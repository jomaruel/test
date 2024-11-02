
--[[===================================================================================================
	 batch.lua
	 
	 Module to be "required" in a DV listener to magically turn an on-line sequence into a batch sequence
     Provides the same functionality as the Modellica's Decision Engine but inside DV
     It is operating system independent and doesn't depend on DV version(2.x, 3.x)as long as the
     internal DV API functions are unchanged
     
     It has a per-project configuration file to set its running parameters (see read_configuration)
          
     The module has only to public methods:
     - batch.init()
     - batch.more_lines()
     +
     and a variable
     -batch.stop_processing
     
     Everything else is private
     
     It may be  optimized for speed a lot but don't bother optimizing any function other than 
     batch.more_lines() (and the functions called by it) as it is the only function that will be called 
     multiple(possibly millions) of times
     It doesn't make sense to try to optimize for exemple batch.init() as it is called only once
     
	 To "study" this module start at the botton which contains the two public functions of the module and
     and then go downwards
     The module file size is due to comments more than to source code     
     
	 (c) GDS-Modellica 
	 initial version by JM Ruiz Dic 2023- Feb 2024
    ===================================================================================================--]]

-- Note: file global variables are upper case (MSG_PREFIX, EOF, HTTP_HEADER, SLASH and NL)

MSG_PREFIX = "** Batch: "  --prepend module messages with this
EOF = -1

-- =============================================================================================
-- Section 1: Utilty functions
-- =============================================================================================


-- ===================== *message* ===========================
-- returns a formatted message with %d, %s, etc..
-- call it this way:  message("%d records processed in file %s", line_count, file_name)
--
-- message includes MSG_PREFIX to distinguish DV messages from messages comming from this module 

local function message(msg, ...) 
	return #{...} ~= 0 and ((MSG_PREFIX or '')..msg):format(table.unpack{...}) or ((MSG_PREFIX or '')..msg)
end

-- ===================== *DV output functions* ===========================
local function dv_err(msg, ...)  OutputErrorString(message(msg, ...))  end
local function dv_info(msg, ...) OutputInfoString(message(msg, ...))  end
local function dv_warn(msg, ...) OutputWarnString(message(msg, ...))  end

-- ===================== *append_slash* ===========================
-- appends a [slash] to [dir] if it hasn't one. [slash] may be "/" or "\\"
local function append_slash(dir, slash)    
    if slash == nil then         -- sanity check, should not happen
        set_os_separators()   
        slash = SLASH
    end    
    if dir and not dir:find(slash, #dir, true)  then
        return dir..slash
    end        
    return dir
end

-- ===================== *exists* ================================
-- checks if [file] or directory exists. The trick os.rename(file, file) 
-- is taken from the WEB and works in Linux and Windows(even the 13 
-- error code that indicates that the file exists but permission is 
-- denied is the same in both systems )
-- returns true/false and error message if any 
local function exists(file, is_a_dir, slash)
    if file then 
        if is_a_dir then file = append_slash(file, slash) end		
        local ok, err, code = os.rename(file, file)
		if code == 13 then ok = true end
        return ok, err
    end  
    return false, ""
end
-- ===================== *trim* ================================
local function trim(s)
	return s:match( "^%s*(.-)%s*$" )
end
-- ===================== *set_os_separators* ====================
-- sets the globals SLASH and NL that depend on operating system 
local function set_os_separators()  
    local c, islinux, iswindows
    
    -- the easy way
    if package.config then
        c = package.config:sub(1,1)
        if c == "/" then
            islinux   = true    
            iswindows =  false
        elseif c == "\\"  then
            islinux  = false                
            iswindows = true 
        end        
    end    
    if c == nil then
        -- alternate method if package.config not set for any reason
        iswindows = os.getenv('WINDIR') or (os.getenv('OS') or ''):match('[Ww]indows')
        islinux = not iswindows and exists("/proc", true, "/")
    end  
    if islinux  then
        NL    = '\n'
        SLASH = "/"
    elseif iswindows  then
        NL    = '\r\n'
        SLASH = "\\"    
    end    
    return iswindows, islinux     
end 

-- ===================== *get_listener* =======================
-- returns listener info for [listener_id] or nil if [listener_id] doesn't exist

local function get_listener(listener_id) 
	local l = GDS.GetListeners()	
	for k, v in pairs (l) do			
		if k == listener_id then return v  end
	end		
	return nil
end	
-- ===================== *raise_exception* =======================
-- raise a lua 'protected call' error (used in pcall(batch.init())
local function raise_exception(msg, ...) error(#{...} ~= 0 and message(msg:format(table.unpack{...})) 
                                                           or message(msg))  end

-- ===================== *change_file_extension* =================
local function  change_file_extension(filename, new_extension)        
    local s = filename:gsub("%.[^.]*$", new_extension) 
    if s == filename then return filename .. new_extension end
    return s
end 

-- ===================== *pick_input_file* ===========================
-- picks the most recent file in [path] by opening an operating system pipe

-- note: files with ".1" extension are ignored because input file is given
-- a ".1" extension when processing ends so that it is not processed again
--
local function  pick_input_file(path, islinux)        
  
    local cmd    
    
    if not islinux then         -- windows
        cmd = string.format('dir /B /O-D /A-D "%s"  2>nul', path)
    else
        cmd = string.format('ls -lt -Q | grep ^-  2>null', path)
    end  
    
    local handle = io.popen(cmd)       
    if handle then
        while true do
            local name =handle:read("*l")       
            if name and name ~= '' then
                if islinux then
                    name = name:match('"(.+)"') or ''
                end    
                if name:match("%.1$") == nil  then -- ignore '.1' extension
                    return name
                end
            end    
        end  
    end    
    return nil
 end   

-- ===================== *get_host_and_client* ===============================
-- returns host and client from the HTTP headers. This is not very reliable
-- in general web contexts as HTTP header is easily edited but is enought in
-- the conexts where DV normally runs

local  function get_host_and_client(header)
    
    local client_priority= { "client_ip",  "x_forwarded_for", "x_forwarded",
                             "x_cluster_client_ip","forwarded_for", "forwarded",
                             "via", "remote_addr"}            

	if not header then return "localhost","localhost"  end
	
	--convert header to lower case
    local t = {}
    for k , v in pairs(header) do
        t[string.lower(k)]= v      
    end    
    
	--host is easy
	local host   = t["host"] or "localhost"   
	
	-- for client, traverse variables in priority order (most reliable first)
	-- "x_forwarded_for" probably exists but, caution, it may be a 
	-- comma list of values so account for this
    local client = "localhost"       
    for j = 1, #client_priority do
        local clave = client_priority[j]
        if t[clave] and t[clave] ~= "" then
            if clave == "x_forwarded_for" then
                client= (GDS.StringSplit(t[clave], ","))[1]
            else
                client =  t[clave]    
            end            
            break
        end
    end        
  return host, client
end

-- =====================  *convert_to_num* ===============================
-- like tonumber but returns NaN instead of nil if conversion is not possible
local function convert_to_num(string_value)
	return tonumber(string_value) or 0/0  -- return NaN is conversion  is not posiible
end
-- ===================== *convert_to_string* ===============================
-- convert a number to string  using [config.output_decimals] format and [config.nan_text]

-- note: validate_params() (see later) transforms [config.output_decimals] set by user 
--  into a format string %g or %.xf where x = 0..10 
-- convert_to_string assumes [config.output_decimals] is already the format string

local function convert_to_string(num_value)
	if not num_value or num_value ~= num_value then   --nil and NaN converted to [config.nan_text]
		return  config.nan_text
	end	
     -- normal conversion using user format ( %g (auto) or %.[0-9]f)
	return string.format(config.output_decimals, num_value)	 
end

-- =============================================================================================
-- Section 2: Read user congiguration and check config parameters
-- =============================================================================================

--check functions. return "" (not nil) on success or an error message
local function check_dir(dir, is_input_dir)
    if type(dir) ~= "string" then
        return "Directory must be a string" 
    end
    local ok, err = exists(dir, true, SLASH)
    if ok then
      return ""
    end  
    return string.format("Can't %s this directory (%s)",  is_input_dir and "read" or "write to",  err)    
end

local function check_separator(sep)  
    if type(sep) ~= "string"  then
        return "Separator must be a quoted char" 
    end
    if ("\t;|"):find(sep , v,1, true) then
      return ""
    end  
    return "Must be one in '\\t' ';' '|'"
end

local function check_boolean(val)  
      return type(val) == "boolean" and "" or "Must be boolean true or false (with no quotes)"
end
local function check_listener(listener_id)
	if not get_listener(listener_id)   then
		return ("Listener %d not found"):format(listener_id)
	end
	return ""	
end	

-- here are the config options
-- mandatory params don't need a default value field
-- func returns the result of the checking function + the value of the paramater
-- we return the value because we can modify what the user entered  as with
-- output_decimals
options = {
    input_dir       = { mandatory =true, 
                        func= function(v) return check_dir(v, true), v  end
                      },
    output_dir      = { mandatory =true, 
                        func= function(v) return check_dir(v, false), v  end
                      },
    listener_id     = { mandatory =true, 
                        func= function(v) return check_listener(v), v  end
                      },                      
    input_separator = { default ="\t", 
                        mandatory =false,
                        func= function(v) return check_separator(v), v  end
                      },
    output_separator= { default ="\t",  
                        mandatory =false,
                        func= function(v) return check_separator(v), v  end
                      },            
    nan_text        = { default ="NaN", 
                        mandatory =false,
                        func=  function(v) return "", v  end
                      },
    output_decimals = { default ="%g",
                        mandatory =false,
                        func= function(v) 
                              if v =="auto" then return "", "%g" end                                          
                              local n = tonumber(v) 
                              if not n or n <0  or n >10 then return "Must be 'auto' or a number be between 0 and 10",v end
                              return "", ("%%.%df"):format(n) 
                        end
                      },
    locale =          { 
                        default ="US_en",
                        mandatory =false,
                        func= function(v)                             
                              if type(v) ~= "string" then return "Must be a string", v end
                              if not v:match("[A-Z][A-Z]_[a-z][a-z]") then
                                    return "Must have the form XX_yy (US_en, ES_es, UK_en,...)" ,v
                              end   
                              return "",v
                        end
                      },
                                            
    generate_file     = { 
                        mandatory =true,
                        func= function(v) return check_boolean(v), v end
                      },    
    want_system     = { 
                        default = true,
                        mandatory =false,
                        func= function(v) return check_boolean(v), v end
                      },    
                      
    want_input     = { 
                        default = false,
                        mandatory =false,                           
                        func= function(v) return check_boolean(v), v end
                      }   
                      
    }

-- ===================== *validate_params* =============================== 
local function validate_params()
    
    for k, v in  pairs(options) do       		         
        if config[k]== nil or config[k]== '' then
            if v.mandatory then
               return false, ("Missing mandatory parameter '%s' in configuration file"):format(k) 
            end
            if config[k]== ''  then
                dv_warn("Empty parameter '%s' in configuration file will be taken as '%s'", 
                            k, 
                            v.default=='\t' and '\\t' or v.default)
            end    
            config[k]= v.default 
        else
            local msg, value = v.func(config[k])
            if msg ~= "" then
                return false, ("Wrong value '%s' for parameter '%s' in configuration file: %s"):format(value, k, msg)
            end
            config[k]= value
        end  
    end
    return true
end

-- ===================== *search_config_file* ===============================
-- called when [optional_config_path] is not provided in batch.init(), 
-- we search for the configuration file in a ';' separated list [path_list] 
-- specified in  package.path or in the env variable LUA_PATH
-- we search the file taking its directory like this:
-- 1. if the component in [path_list] does not include ? , this is a candidate 
--    directory in which to look for the configuration file
-- 2. if the component in [path_list] includes ? , but ends in ?.lua, ?.dll, etc...
--    this is also a directory candiate to look for the configuration file replacing
--    ?.lua, ?.dll, etc.. by [config_name]

local  function search_config_file(path_list, config_name)
    local t = GDS.StringSplit(path_list , ";")
    for j = #t, 1, -1 do
        local question_mark_exists = string.find(t[j], "?", 1, true)
        if not question_mark_exists or string.find(t[j],  "%?%..+$") then
            local s
            if question_mark_exists then
                s = string.gsub(t[j], "%?%..+$", "")..config_name
            else
                s = append_slash(t[j], SLASH)..config_name
            end   
            if exists(s, false) then
                return s
            end  
        end
    end    
    return nil
end
-- ===================== *read_configuration* ===============================
-- if [optional_config_path has been specified], use it
-- otherwise call [search_config_file]
-- returns boleans OK + error_msg if any

local function read_configuration(optional_config_path)
    
    local config_name = (GDS.GetSystemName() or '') .."_conf.lua" 
    local config_file  
        
    if SLASH == nil then
        set_os_separators()      --sanity
    end    
    
    
    if optional_config_path then
    	config_file = append_slash(optional_config_path, SLASH)..config_name 
        if not exists(config_file, false, SLASH) then
            return false, "Can't find configuration file '"..config_file.."'"
        end    
    else
        local path_string = os.getenv("LUA_PATH")
        if not path_string or  path_string == "" then
           path_string = package.path
        end    
        if path_string and  path_string ~= "" then
			config_file = search_config_file (path_string, config_name)
            if config_file == nill then
                return false, "Can't find configuration file '"..config_name.."'"
            end    
        end    
    end        
    
    if config_file == nil then
        return false, string.format("Can't find %s. 'package.path' and 'LUA_PATH' not set "..
                              "and no path to configuration file has been specified in batch.init()",
                              config_name )
    end            
    
    dv_info("Reading batch configuration file: %s", config_file)
    
    -- "run" the congiguration file as a Lua Script. Errors here are caught in the pcall of init
	
    -- simple dofile () provides error reporting and is already integrated into DV
	-- so let's use it
	-- loadfile simply returns a nil function on error with no info about the error
    
    dofile(config_file)

	--[[
    --old method:
    local func = loadfile(config_file)    
    if not func then
        return false, "Lua syntax error while processing configuration file "..config_file..". (Check the comma at the end of each line)"
    end  
    func()
	--]]
    
    return validate_params()

end
-- =============================================================================================
-- Section 3: File operations (can be optimized)
-- =============================================================================================
local File = {
    in_handle   = nil,
    out_handle  = nil,
    in_name  = "",
    out_name   = "",
    line_count = 0,
    read_size = 128*1024, 	--	use very big  numbers for perfomance
    write_size=128*1024,	--	use very big  numbers for perfomance
    buf_is_empty = true,    --  probably 128*1024 is too big  and doesn't make much difference with 32*1024
    buf = '',
    pos   = 1,
    fields = {},
    num_fields,
    system_values = nil,
	header_positions = {}  -- map[header_variable_name] =>field_number
}

-- ===================== *File.initialize* ===============================
-- we need to initialize these fields on init (apart from being initialized 
-- in File object) because we don't know if inside the workbench module vars 
-- keep their values between runs, so after the first call to batch.init()
-- File values might be  wrong
-- delete in not needed

function File.initialize()
	File.system_values = nil
	File.buf_is_empty = true
	File.buf = ''
	File.pos   = 1
	File.fields = {}
	File.line_count = 0
end
-- ===================== *open_for_read* ===============================
function File.open_for_read(name)
    File.in_name   = append_slash(config.input_dir, SLASH) .. name
    File.in_handle = io.open(File.in_name, "rb")        
    return File.in_handle ~= nil
end  
-- ===================== *open_for_write* ===============================
function File.open_for_write(name)
    File.out_name   = append_slash(config.output_dir, SLASH) .. name
    File.out_handle = io.open(File.out_name, "wb")  
    if  File.out_handle then 
        File.out_handle:setvbuf("full", File.write_size)   		
    end    
    return File.out_handle ~= nil
end
-- ===================== *get_line_limits* ===============================
-- returns start and end positions of the current line in the input buffer
-- end of line marker in the input file can be '\n' or '\r\n'
-- end position doesn't include the end of line marker
-- On end of file start postion is set to EOF (-1)

function File.get_line_limits()
  
    local p
    local line_end
    local eof = false
    local current_pos = File.pos
    local buf = File.buf

    if File.buf_is_empty then
        --
        -- special Lua construct: this won't read a line (despite of the "*l")
        -- it will read as many lines as [read_size] permits. It also ensures 
        -- that the buffer contains an integral number of full lines (no partial lines)
        -- I took this strange read form from R. Lerusalimschy(Lua creator) but I 
        -- don't remenber where it was
        
        local lines, rest = File.in_handle:read(File.read_size, "*l")
        if not lines then
                return EOF,EOF
        end
        if rest then 
            buf = lines .. rest .. '\n' 
        else
            buf = lines 
        end        
        current_pos = 1   
        File.buf_is_empty  = false
        
    end
    File.line_count = File.line_count + 1 
    p = string.find(buf, "\n", current_pos, true)        
    if p  then
        line_end = p - 1        
        if buf:byte(line_end)==13 then
            line_end = line_end - 1
        end  
        if p == #buf then
            File.buf_is_empty = true
        else    
            File.pos = p + 1          
        end    
    else
        line_end = #buf
        File.buf_is_empty = true
    end 
    File.buf = buf
    return current_pos, line_end
end 

-- ===================== *File.parse_line* ===============================
-- splits a line into fileds and returns # of fields
-- string.sub(File.buf, start, end_) is slow
-- if we had a function GDS.BufferSplit(buf, separator, start_pos, end_pos) things would
-- go much faster
function File.parse_line()
    local start, end_ = File.get_line_limits()
    if start ~= EOF then    
        File.fields = GDS.StringSplit(string.sub(File.buf, start, end_), config.input_separator)    
        return #File.fields    
    end
    return EOF
end
-- ===================== *File.get_header_indexes* ===============================
-- takes t[1] = header_field1, t[2] = header_field2, ..
-- and returns a table like this
-- table[header_field1] =1
-- table[header_field2] =2
-- .....
-- checks also for duplicated field names and names with spaces in the middle

function File.get_header_indexes(header_names)    
 	local positions = {}
    for j=1, #header_names do
        local f = trim(header_names[j]) -- alow leading and trailing spaces  but not in the middle
        if positions[f] ~= nil then
            raise_exception("Duplicate input field '%s'", f) 
        end
        if string.find(f, " ", 1, true) ~= nil then
            raise_exception("Field '%s' cannot contain spaces", f) 
        end
        positions[f] = j
    end  
	return positions
end

function File.write_table(fields, terminator)    
    File.out_handle:write(table.concat(fields, config.output_separator))
    File.out_handle:write(terminator)   
end

function File.write_system_labels()
    File.write_table({"TimeStamp","System Name","System Id","System Version","Host","Client"},config.output_separator)
end

function File.write_system_values(input_headers)
    -- On the first call we build system values except timestamp
    -- Other values do not change so we fill them only once (On the first call to write_system_values)
    -- Only timestamp needs to be added on each output line
    if not File.system_values then   		
		local host, client =  get_host_and_client(input_headers)
	
        File.system_values = {
            "", -- placeholder for timestamp
            GDS.GetSystemName(),
            GDS.GetSystemId(),
            GDS.GetSystemVersion(),
			host, 
			client
        }  
	end	
    File.system_values[1] =  os.date('%Y%m%d %H:%M:%S')     
    File.write_table(File.system_values, config.output_separator)
end

-- =============================================================================================
-- Section 4: DataView functions
-- This is the complicated section
-- =============================================================================================

local DV = {
    
    -- 
    -- [out_field_info] and [inf_field_info] are vectors whose index is determined by the traversal
    -- order of input and output records.  This is order is unrelated to the order in which 
    -- input fields appear in input file
    -- That's why we need [node_index_2_file_index] that maps node indexes to file indexes
    
    in_field_info = {},        
    out_field_info = {},        
    node_index_2_file_index = {},
}    

--[[ =============================================================================================
                                    ** get_fields_info **
                                    
                          **** THIS IS THE KEY FUNCTION OF THE MODULE **** 
    
	Recursively scan gds record starting at node whose id and table are (node_id,table_node) and
	collect information for these elements:
	
		- fields
		- nodes that have jsonNodeType ="Array" (this is for vectors in the input file, AA#BB#CC..)
	
	get_fields_info is called twice on init()
	
	one for input record 
	one for ouput record 
	
	
	The function returns the out parameter "field_info" (not returned as function result due to recursion)	
	
	field_info is a vector representing each element with the following attributes that will allow later to
	read the element from the input file or written to the output file
	
	field_info[index].table_node
	field_info[index].key
	field_info[index].is_simple_field (true for fields, false for arrays)
	field_info[index].func (the conversion func that converts the value of an element from number to string or vice versa)
    field_info[index].display_name
    
    so the value of an element is at info.table_node[info.key]
    where info = field_info[index]
    
    index is the sequential index of the element built on record traversal
	
    =============================================================================================--]]
function get_fields_info(node_id, table_node, is_input, field_info)

	local node = GDS.GetNode(node_id)
	header_postions =  File.header_postions -- to link node indexes with file indexes
	
	for k, v in ipairs(node.children) do
        
        local element, prop
        local element_is_field
        
        if v.type == "field" then
            element = GDS.GetField(v.id)
            element_is_field = true
        else
            element= GDS.GetNode(v.id)
            element_is_field = false
        end
        prop =  element.properties 
        
        -- fields or nodes  of type Array
        if element_is_field or (prop.occurrenceType =="Multiple" and prop.jsonNodeType == "Array") then
				local info = {}
				local fieldtype
                if element_is_field then
                    info.table_node = table_node
                    info.key = prop.name
                    info.is_simple_field =  true
					fieldtype = prop.fieldtype
                else
					local children = (element.children or {})[1]
					if children then			
	                    info.table_node = table_node[v.name]  
	                    info.key = children.name
						info.is_simple_field =  false
						fieldtype = GDS.GetField(children.id).properties.fieldtype
					end	
                end            
				if fieldtype ~= 'numeric' then
					info.func= nil
                elseif is_input then
					info.func= convert_to_num       -- input conversion function    
				else
					info.func= convert_to_string    -- output conversion function
				end		
                info.display_name = prop.displayName               
				local n = #field_info + 1				
				
                if is_input then
                    -- map node index to file index
                    DV.node_index_2_file_index[n] = header_postions[prop.displayName] 
                end    
				field_info[n] = info							

        elseif element_is_field == false then          -- recursion for nodes that are not Arrays
            get_fields_info(v.id, table_node[v.name], is_input, field_info)
        end    
	end		
end	

function get_in_fields_info(node_id, table_node)
	local result = {} -- don' t forget to clear the output param or disaster
	get_fields_info(node_id, table_node, true, result)	
	return result
end

function get_out_fields_info(node_id, table_node)
	local result = {}
	get_fields_info(node_id, table_node, false, result)
	return result
end

-- ===================== *set_node_values* ===============================
-- populate the input record with the file_fields 
-- This guided by DV.in_field_info and DV.node_index_2_file_index 
-- clears previous array values. Simple field values don't need to be cleared 
-- if we ensure that file lines (other than header) have the same # of fields
-- see note on this in batch.more_lines()

function set_node_values(file_fields)	
	
    local in_field_info = DV.in_field_info
    local raw_file_values = {}
    local node_index_2_file_index = DV.node_index_2_file_index
    
    for j = 1, #in_field_info do

		local file_value = file_fields[node_index_2_file_index[j]]  
		
        local info = in_field_info[j]	
        local key  = info.key
		local func = info.func
		
		    
		if info.is_simple_field then       	
			
            if func == nil then
                info.table_node[key] =  file_value 
            else
                info.table_node[key]= func(file_value)
            end                
		else                    -- array node            
			local t = GDS.StringSplit(file_value or '', '#')			
			for i = 1,#t do
				info.table_node[i][key] = func == nil and t[i]  or func(t[i])
			end				
			--clear previous json array value
			for i = #t+1, #info.table_node do
				info.table_node[i]:Delete()
			end			
		end
	end       
end

-- ===================== *clear_gds_record* ===============================
-- called for gds output record when [field_info] is DV.out_field_info
-- *not* called for gds input record when  [field_info] is DV.in_field_info


function clear_gds_record(field_info)
    
	for j = 1, #field_info do	
		local info = field_info[j]	
		
		if info.is_simple_field then
			info.table_node[info.key]  = nil
		else
			info.table_node:ForEach(function (elem)
										elem:Delete()
									end)								
		end							
	end

end	
-- ===================== *get_node_values* ===============================
-- return a lua table with the values of the nodes. This table is the one
-- writen to utput file

-- called for gds input record when  [field_info] is DV.in_field_info
-- called for gds output record when [field_info] is DV.out_field_info

function get_node_values(field_info)
    	
    local out_values = {}
	for j = 1, #field_info do
	
		local info = field_info[j]	
        local key  = info.key
		local func = info.func
		
		if info.is_simple_field then
			local value = info.table_node[key]  or ''
			if func == nil then
				out_values[j] = value
			else
				out_values[j] = func(value)
			end	
		else
			local s = ""
			info.table_node:ForEach(function (elem)
									local value = elem[key] or ''										
									if func == nil then
										s = ("%s%s#"):format(s, value)
									else
										s = ("%s%s#"):format(s, func(value))
									end	
									end)								
			out_values[j] = s			
		end
	end
    return out_values
end	

-- ===================== *get_header_names* ===============================
-- get display names of nodes that will be writen as headers in the output file
function get_header_names(node_field_info)
    local t = {}
	for j = 1, #node_field_info do
        t[j] = node_field_info[j].display_name
	end
    return t
end	

-- =============================================================================================
-- Section 5: bach.init and bach more_lines
--
-- in addition to batch.init and batch.more_lines, there is also a boolean [batch.stop_processing]
-- variable
-- it is needed to suspend processing (=exit batch.more_lines) if there are errors on the listner
-- call to init
-- withoud it, the sequence would run anyway
-- =============================================================================================

batch = {}

-- ===================== *internal_init* ===============================
-- helper function called by batch.init(). Does all the work. Raises
-- exceptions that are caught in batch.init()
-- returns nothing

-- the gds_record record (=record) param is not probably needed but I don't know
-- to pass the gds record  information of the listener to an external module so I used
-- this trick

function internal_init(gds_record, optional_config_path)

    local continue = true
    local name, msg
    
    --==========================================================================================
	-- set separators (SLASH and NL). This must be the first function called
	--==========================================================================================
    local iswindows, islinux = set_os_separators()
	
    if not iswindows and not islinux then
        raise_exception("Could not detect OS") 
    end
    --==========================================================================================
	-- read configuration
	--==========================================================================================    
    continue, msg = read_configuration(optional_config_path)
    if not continue then
        raise_exception(msg) 
    end
	--==========================================================================================
	-- set locale
	--==========================================================================================
	if os.setlocale(config.locale) ~= config.locale then
		dv_warn("Couldn't set locale to '%s'. The systmem will continue with locale '%s'", 
				 config.locale, os.setlocale())
	end
    --=============================
    -- Is there a file to process?
    --=============================    
    name = pick_input_file(config.input_dir, islinux)
    if not name  then
        dv_info("No file to process ")
		batch.stop_processing = true
        continue = false
    end    
    
    if continue then           
	    -- var  upper case = global var
		HTTP_HEADERS = (GDS.GetListenerData(config.listener_id) or {}).input_headers
		
		--==========================================================================================
		-- get root nodes for the listener
		--==========================================================================================
	    local batch_listener  = get_listener(config.listener_id)

		local input_node = batch_listener.input[1]
		local output_node= batch_listener.output[1]  
	    if not input_node then
			raise_exception("Input node not found for listener %d", config.listener_id)
		end
		if not output_node then
			raise_exception("Output node not found for listener %d", config.listener_id)
		end        
    
	    --=============================
	    -- Open in and out files
	    --=============================

        if not File.open_for_read(name) then 
            raise_exception("Could not open '%s' for reading", config.output_dir) 
        end
		
        dv_info("Processing input file %s",File.in_name)
    
        if config.generate_file then            
            if not File.open_for_write(change_file_extension(name, ".out")) then              
                raise_exception("Could not open '%s' for writing", File.out_name)
            end                  
            dv_info("Generating output file %s",File.out_name)
        end    
        --========================================================================
        -- read input header. This must be done here because the header is not a 
        -- true record and we don't want later the function more_lines() to be
        -- affected
        --======================================================================== 
        n = File.parse_line()          
        -- reset line_count to 0 (its value is 1  after reading the header)        
		File.line_count = 0
        
        --========================================================================
        -- collect the field number of file header names
        --========================================================================         
        File.header_postions= File.get_header_indexes(File.fields)
        
		--========================================================================
        -- get information of the fields/arrays in input node
        --========================================================================         
		local listener_name =  batch_listener.properties.name       
		local itab= gds_record[listener_name]["input"][input_node.name]		
		
		
        DV.in_field_info = get_in_fields_info(input_node.id, itab)		
        
        if config.generate_file then   
			--========================================================================
	        -- get information of the fields/arrays in output node
	        --========================================================================         		     	
			local otab= gds_record[listener_name]["output"][output_node.name]
            DV.out_field_info = get_out_fields_info(output_node.id,otab)

            --========================================================================
            -- write system vars? if so write system header vars here
            --=======================================================================            
            if config.want_system then				
                File.write_system_labels()    
            end    
            --========================================================================
            -- -- write input vars? if so write input header vars here
            --=======================================================================
            if config.want_input then
                File.write_table(get_header_names(DV.in_field_info), config.output_separator)
            end    
            --========================================================================
            -- always write output header
            --=======================================================================            
            File.write_table(get_header_names(DV.out_field_info), NL)            
        end             
    end    
end
-- ===================== *batch.init* ===============================
function batch.init(gds_record, optional_config_path)

	File.initialize()		-- needed?
    local ok, msg = pcall(internal_init,gds_record, optional_config_path)
    
	batch.stop_processing = not ok
    
	if not ok then dv_err(msg)  end
    return ok
end 

-- ===================== *finish* ===============================
-- not called by user . more_lines() calls it on EOF or error
function finish()    
    if  File.in_handle then 
		io.close(File.in_handle) 
		os.rename(File.in_name, change_file_extension(File.in_name, ".1"))
	end
    if  File.out_handle then io.close(File.out_handle) end
    
    dv_info("%d records processed", File.line_count)	
end 


-- ===================== *batch.more_lines* ===============================
-- 
function batch.more_lines()
  
    local line_count = File.line_count

    if batch.stop_processing then
        finish()
        return false
    end

    --======================================================================
    -- before reading a new line, the previous line is pending 
    -- to be procesed and written.Do it now 
    -- It must be line_count > 0 as the header doesn't count as a line 
    -- line_count has been set to 0 after reading the header
    --======================================================================
		
    if line_count > 0 and config.generate_file then  
        --========================================================================
        -- write system values to output file ?
        --=======================================================================                
        if config.want_system then       		
            File.write_system_values(HTTP_HEADERS)
        end		
        --========================================================================
        -- write input values to output file ?
        --=======================================================================                        
        if config.want_input then   			            
			File.write_table(get_node_values(DV.in_field_info), config.output_separator)
        end    
        --========================================================================
        -- values of output node always written
        --=======================================================================                                
        File.write_table(get_node_values(DV.out_field_info), NL)  
		--dv_info("DEBUG WRITE LINE %d:'%s'", File.line_count, table.concat(File.fields, config.output_separator))		
    end        
    --======================================================================
    -- read a new line
    --======================================================================    
    local n = File.parse_line()    
	
    --======================================================================
    -- No more lines ?
    --====================================================================== 
    if n == EOF then
        finish()
        return false
    end
	--dv_info("DEBUG READ LINE %d:'%s'", File.line_count, table.concat(File.fields, config.output_separator))

	--======================================================================
    -- clear gds output record
    -- Note:
	-- we don't need to clear the input record thanks to the optimization
	-- described at the end of the function. 
	-- Uncomment the line "clear_gds_record(DV.in_field_info") if you want
	-- to also clear the the input record but this only slows down perfomance
    --======================================================================     		
    		
    clear_gds_record(DV.out_field_info)	
	--clear_gds_record(DV.in_field_info) 
	
	--======================================================================
    -- set gds input record from the fields_values we have just read
    --======================================================================     	
	set_node_values(File.fields)
	    
    --======================================================================
    -- check that the number of fields is the same on every line. 
	-- This is not only a file integrity check. It is also an optimization since
	-- we don't need to clear the gds input record  for each line read
	-- Well, we need to clear Json arrays anyway but this is done in
    -- set_node_values. If you supress checking the # of fields you will need 
	-- to call clear_gds_record(DV.in_field_info) above
    --======================================================================    
    if line_count > 1 and n ~= File.num_fields  then
        dv_error("line %d has %d fields but line %d has %d fields", line_count, n, line_count - 1, File.num_fields )
        
        return false
    end        
    -- uptate num_fields for next read
    File.num_fields  = n
	
    
    return true
end



