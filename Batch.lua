-- esto viene de hijo
MSG_PREFIX = "** Batch: "  --prepend module messages with this
EOF = -1

-- =============================================================================================
-- Section 1: Utilty functions
-- =============================================================================================


-- ===================== *message* ===========================
-- call it this way:  message("%d records processed in file %s", line_count, file_name)

local function message(msg, ...) 
	return #{...} ~= 0 and ((MSG_PREFIX or '')..msg):format(table.unpack{...}) or ((MSG_PREFIX or '')..msg)
end
-- ===================== *DV output functions* ===========================
local function dv_err(msg, ...)  OutputErrorString(message(msg, ...))  end
local function dv_info(msg, ...) OutputInfoString(message(msg, ...))  end
local function dv_warn(msg, ...) OutputWarnString(message(msg, ...))  end


-- ===================== *append_slash* ===========================
-- append a slash to dir if it hasn't one. slash may be "/" or "\\"
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
-- check if file or directory exists. The trick os.rename(file, file) 
-- is taken from the WEB and works in Linux and Windows even de 13 
-- error code that indicates that the file exists but permission is denied
-- returns Ok and error message 
local function exists(file, is_a_dir, slash)
    if file then 
        if is_a_dir then file = append_slash(file, slash) end		
        local ok, err, code = os.rename(file, file)
		if code == 13 then ok = true end
        return ok, err
    end  
    return false, ""
end

function trim(s)
	return s:match( "^%s*(.-)%s*$" )
end
-- ===================== *set_os_separators* ====================
-- sets the globals SLASH and NL that depend on operating system 
function set_os_separators()  
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

local function get_listener(listener_id)  
	local l = GDS.GetListeners()	
	for k, v in pairs (l) do			
		if k == listener_id then return v  end
	end		
	return nil
end	
-- ===================== *raise_exception* =======================
-- raise a lua 'protected call' error (used in pcall(Batch.init())
local function raise_exception(msg, ...) error(#{...} ~= 0 and message(msg:format(table.unpack{...})) or message(msg))  end

-- ===================== *change_file_extension* =================
local function  change_file_extension(filename, new_extension)        
    local s = filename:gsub("%.[^.]*$", new_extension) 
    if s == filename then return filename .. new_extension end
    return s
end 

-- ===================== *pick_input_file* ===========================
-- pick the most recent file in [path] by opening an operating system pipe
-- note: files with ".1" extension are ignored because input file is given
-- a ".1" extension when processing ends so that it is not processed again
--
function  pick_input_file(path, islinux)        
  
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
-- =====================  *convert_to_num* ===============================
-- like tonumber but returns NaN instead of nil if conversion is not possible
--
local function convert_to_num(string_value)
	return tonumber(string_value) or 0/0  -- return NaN is conversion  is not posiible
end

-- ===================== *convert_to_string* ===============================
-- convert a number to string  using [config.output_decimals] format and [config.nan_text]
-- note: validate_params() transforms [config.output_decimals] set by user  into a format string 
-- %g or %.xf where x = 0..10 
-- convert_to_string assumes [config.output_decimals] is already the format string

local function convert_to_string(num_value)
	if not num_value or num_value ~= num_value then   --nil and NaN converted to [config.nan_text]
		return  config.nan_text
	end	
     -- normal conversion using user format ( %g (auto) or %.[0-9]f)
	return string.format(config.output_decimals, num_value)	 
end

-- for these two functions it would be safer to convert first http_header to upper case and
-- then check for "HOST" and "X-FORWARDED-FOR"
-- Also get_client can be more sofisticated if we check not only X-FORWARDED-FOR but
-- also CLIENT, REMOTE_ADDR, etc...

-- ===================== *get_host* ===============================
local function get_host(http_header)    
    return http_header["host"] or "localhost"
end
-- ===================== *get_client* ===============================
local function get_client(http_header)    
    return http_header["x-forwarded-for"]or "localhost"
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
    return string.format("Can't %s directory '%s' (%s)", 
				is_input_dir and "read" or "write to", 
				dir,
				err)    
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
-- ===================== *read_configuration* ===============================
local function read_configuration(optional_config_path)
    
    local config_name = (GDS.GetSystemName() or '') .."_batch.lua" 
    local config_file  
        
    if SLASH == nil then
        set_os_sparators()      --sanity
    end    
    --========================================================================
    -- if optional_config_path has been specified, use it
    -- otherwise take the **last** component of package.path or "LUA_PATH"
    --======================================================================== 
    --if optional_config_path then
    	--config_file = append_slash(optional_config_path, SLASH)..config_name 
    --else
        local path_string = os.getenv("LUA_PATH")
        if not path_string or  path_string == "" then
           path_string = package.path
        end    
        if path_string and  path_string ~= "" then
            local t  = GDS.StringSplit(path_string, ";")
            config_file = append_slash(t[#t], SLASH)..config_name
        end    
    --end        
    if config_file == nil then
        return false, "'package.path' and 'LUA_PATH' not set and no path to configuration file has been specified in init()"
    end            
    if not exists(config_file, false, SLASH) then
          return false, "Can't open configuration file '"..config_name.."'"
    end    
    dv_info("Reading batch configuration file: %s", config_file)
    
    local func = loadfile(config_file)    
    if not func then
        return false, "Error processing configuration file (syntax error): "..config_file
    end  
    func()
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
    read_size = 128*1024,
    write_size=128*1024,
    buf_is_empty = true,
    buf = '',
    pos   = 1,
    fields = {},
    num_fields,
    system_values = nil
}

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
-- return start and end of the current line in the input buffer
-- line marker in the input file can be '\n' or '\r\n'
-- end doesn't include the end of line marker
-- On end of file start is set to EOF (-1)

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
        -- I took this strange read form from Lua creators but I don't remenber where it was
        
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

-- ===================== *split* ===============================
-- the module perfomance depends heavily on this function !

-- t = GDS.StringSplit(str, sep) is split(str, sep, 1, #str, t)
-- So why not using StringSplit instead of split?

-- 1.  StringSplit creates a new table each time, split reuses the same table. This is possible
--     because we require all the lines to have the same # of fields (dv_error if not)
-- 2.  StringSplit would force us to first split the input buffer into lines, and then split each
--     line into fields. We prefer to avoid the first split by working with directly with buffer 
--     and line pointers avoiding the line concept

function  split(buf, separator, init_pos, limit_pos, out_table)
    
    local n=  1
	
    while true do
        local i = string.find(buf, separator, init_pos, true)
        if  i  and i <= limit_pos  then
            out_table[n] = string.sub(buf, init_pos, i-1)      
            init_pos = i +1
            n = n + 1        
			
        else    
            out_table[n] = string.sub(buf, init_pos, limit_pos)  
            break
        end    
    end   
     
    return n
end 

function File.parse_line()
	
    local start, end_ = File.get_line_limits()	
    if start ~= EOF then    		
      return split(File.buf, config.input_separator, start, end_, File.fields)      
    end
    return EOF
end

function File.parse_line2()
    local start, end_ = File.get_line_limits()
    if start ~= EOF then    
        File.fields = GDS.StringSplit(string.sub(File.buf, start, end_), config.input_separator)    
        return #File.fields    
    end
    return EOF
end
function File.clear_fields()
    File.fields = {}
    File.line_count = 0
end


function File.write_table(fields, terminator)    
    File.out_handle:write(table.concat(fields, config.output_separator))
    File.out_handle:write(terminator)   
end

function File.write_system_labels()
    File.write_table({"TimeStamp","System Name","System Id","System Version","Host","Client"},config.output_separator)
end

function File.write_system_values()
    -- On the first call we build system values except  timestamp
    -- Other values do not change so we fill them only once (On the first call to want_system_values)
    if not File.system_values then       
        File.system_values = {
            "", -- placeholder for timestamp
            GDS.GetSystemName(),
            GDS.GetSystemId(),
            GDS.GetSystemVersion(),
            get_host(listener.input_headers),
            get_client(listener.input_headers)
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
	file_values = {}     -- this not the same as File.fields as not all File.fields might be 
                         -- present in the gds input record  and File.fields and DV.file_values 
                         -- have probably different orders. 
						 -- [file_values] only used if input fields
                         -- is desired in output file
}    

--[[ =============================================================================================
                                    ** get_fields_info **
                                    
                          **** THIS IS THE KEY FUNCTION OF THE MODULE **** 
    
	Recursively scan gds record starting at node whose id and table are (node_id,table_node) and
	collect information for these elements:
	
		- fields
		- nodes that have jsonNodeType ="Array" (this is for vectors in the input file, AA#BB#CC..)
	
	get_fields_info is called twice on init()
	
	one for input record (header_fields ~= nil),
	one for ouput record (header_fields == nil)
	
	On input, only info for elements of the input gds record that are present in the header file is 
    collected
	
    On output, info for every element of the output gds record is collected
	
	The function returns the out parameter "field_info" (not returned as function result due to recursion)	
	
	field_info is a vector representing each element with the following attributes that will allow later to
	read the element from the input file or written to the output file
	
	field_info[index].table_node
	field_info[index].key
	field_info[index].single (true for fields, false for arrays)
	field_info[index].func (the conversion func that converts the value of an element from number to string or vice versa)
    field_info[index].display_name
    
   index is the sequential index of the element built on gds record traversal
	
    =============================================================================================--]]
	

function get_fields_info(node_id, table_node, header_fields, field_info)

	local node = GDS.GetNode(node_id)
	
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
            local index,fieldtype
            if  header_fields == nil then                   --output record
                index = 0
            else                                            -- input record
                index = header_fields[prop.displayName]     -- search display name in file header
                if index == nil then index = -1 end         -- ignore if not found
            end
            if index >=0 then            -- output element or input element present in file header
				local info = {}
                if element_is_field then
                    info.table_node = table_node
                    info.key = prop.name
                    info.single =  true
					fieldtype = prop.fieldtype
                else
					local children = (element.children or {})[1]
					if children then			
						put("table_node")
						--vput(table_node)
						put("end table_node")	
																		
	                    info.table_node = table_node[v.name]  
	                    info.key = children.name
						info.single =  false
						fieldtype = GDS.GetField(children.id).properties.fieldtype
					end	
                end            
				if fieldtype ~= 'numeric' then
					info.func= nil
				elseif header_fields ~= nil then
					info.func= convert_to_num       -- input conversion function    
				else
					info.func= convert_to_string    -- output conversion function
				end		
                info.display_name = prop.displayName
               
				local n = #field_info + 1				
                
                if index > 0 then
                    DV.node_index_2_file_index[n] = index   				
                end    
				field_info[n] = info							
            end  
            
        elseif element_is_field == false then          -- recursion for nodes that are not Arrays
            get_fields_info(v.id, table_node[v.name], header_fields, field_info)
        end    
	end		
end	

--[[ ====================================================================================
                                    ** set_node_fields **
     
     populate the input record with the file_fields 
     This guided by DV.in_field_info and DV.node_index_2_file_index
     
     if [keep_file_values] == true, store in the traversal order file_fields into DV.file_values 
     but only those that are present in input record
     ====================================================================================--]]

function set_node_fields(file_fields, keep_file_values)	
	
    local in_field_info = DV.in_field_info
	
	--[[
	for j =1,#in_field_info do
		put(j, " - Input)")
		vput(in_field_info[j])
	end	
	--]] 

    local file_values = {}
    local node_index_2_file_index = DV.node_index_2_file_index
    
    for j = 1, #in_field_info do

		local file_value = file_fields[node_index_2_file_index[j]]  
		
        local info = in_field_info[j]	
        local key  = info.key
		local func = info.func
		
		    
		if info.single then        --simple field			
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
			
			-- clear previous values if any
			for i = #t+1, #info.table_node do
				info.table_node[i]:Delete()
			end
			
		end
        if keep_file_values then
            file_values[j] = file_value            
        end    
	end    
    DV.file_values = file_values
    
end
--[[ ====================================================================================
                                    ** get_node_fields **
     ====================================================================================--]]
function get_node_fields()
    local out_field_info = DV.out_field_info
	
    local out_values = {}
	for j = 1, #out_field_info do
	
		local info = out_field_info[j]	
        local key  = info.key
		local func = info.func
		
		if info.single then
			if func == nil then
				out_values[j] = info.table_node[key] 
			else
				out_values[j] = func(info.table_node[key])
			end	
		else
			local s = ""			
			info.table_node:ForEach(function (elem)
										if func == nil then
											s = ("%s%s#"):format(s, elem[key])
										else
											s = ("%s%s#"):format(s, func(elem[key]))
										end	
									end)
			out_values[j] = s
			
		end
	end
    return out_values
end	

function get_header_names(field_info)
    local t = {}
	for j = 1, #field_info do
        t[j] = field_info[j].display_name
	end
    return t
end	

--[[=============================================================================================
	 get root nodes of this listener_id
    =============================================================================================--]]
function root_nodes(listener_id)
	local l = GDS.GetListeners()	
	for k, v in pairs (l) do			
		if k == listener_id then
			return v.input[1], v.output[1]  
		end
	end		
	return nil, nil
end	
function get_input(rec)
	--return rec.ws.input
end
function get_ouput(rec)
	--return rec.ws.output
end

Batch = {}

function internal_init(input_, output_)

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
    continue, msg = read_configuration()
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
        continue = false
    end    
    
    if continue then                
	
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
	    -- Open in and out files?
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
        -- true record and we don't want later the function more_records() to be
        -- affected
        --======================================================================== 
        n = File.parse_line()  
        
        
        --========================================================================
        -- check for duplicated fields and fields that contain spaces
        --========================================================================         
        local header = {}
        for j=1, n do
            local f = trim(File.fields[j])
            if header[f] ~= nil then
                raise_exception("Duplicate input field '%s'", f) 
            end
            if string.find(f, " ", 1, true) ~= nil then
                raise_exception("Field '%s' cannot contain spaces", f) 
            end
            header[f] = j
        end
        
        -- at thispoint File.fields contains the file header, not needed anymore. 
        -- Also reset line_count to 0 (its value is 1  afetr reading the header)
        -- to be processed

        
        
		File.clear_fields()
		
		--local itab = get_input()
		--local otab = get_ouput()
		--itab= batch_listener.input[1]
		--otab= batch_listener.output[1]
		l = get_listener(24)
		--vput(l.properties)
		
		itab= input_[l.properties.name]["input"][input_node.name]
		otab= input_[l.properties.name]["output"][output_node.name]
		
		--itab= input_[input_node.name]
		--otab= output_[output_node.name]
		--otab= config_path["output"][output_node.name]
		
		--otab= batch_listener.output[1]
		
        get_fields_info(input_node.id, itab, header, DV.in_field_info)		  
		for j=1, # DV.in_field_info do
			--put(j, ")))")    			
			--vput(DV.in_field_info[j])
		end	
        
        if config.generate_file then        	
            get_fields_info(output_node.id,otab, nil, DV.out_field_info)	            
			
            --========================================================================
            -- write system header ?
            --=======================================================================            
            if config.want_system then
				
                File.write_system_labels()    
            end    
            --========================================================================
            -- write input header ?
            --=======================================================================
            if config.want_input then
                File.write_table(get_header_names(DV.in_field_info), SLASH)
            end    
            --========================================================================
            -- always write output header
            --=======================================================================            
            File.write_table(get_header_names(DV.out_field_info), NL)            
        end     
        
    end    
end

function Batch.init(p1,p2)

	File.initialize()
    local ok, msg = pcall(internal_init,record, p2)
	Batch.init_failed = ok
    if not ok then dv_err(msg)  end
    return ok
end 
function Batch.finish()    -- not called by user     
    
    
    
    if  File.in_handle then io.close(File.in_handle) end
    if  File.out_handle then io.close(File.out_handle) end
    os.rename(File.in_name, change_file_extension(File.in_name, ".1"))
    
    dv_info("%d records processed", File.line_count)
	    File.buf = ''
    File.pos   = 1
    File.line_count = 0

end 



function Batch.more_lines()
  
    local line_count = File.line_count

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
            File.write_system_values()
        end
        --========================================================================
        -- write input values to output file ?
        --=======================================================================                        
        if config.want_input then   
            File.write_table(DV.file_values, config.output_separator)
			--File.write_table(DV.file_values, NL)
        end    
        --========================================================================
        -- values of output node always written
        --=======================================================================                                
        File.write_table(get_node_fields(), NL)        
    end        
    --======================================================================
    -- read a new line
    --======================================================================    
    local n = File.parse_line2()    
    --======================================================================
    -- No more lines ?
    --====================================================================== 
    if n == EOF then
        Batch.finish()
        return false
    end
    --======================================================================
    -- set gds input line values from the fields_values we have just read
    --======================================================================     
	put("Before set_node_fields(File.fields, config.want_input")	
	    set_node_fields(File.fields, config.want_input)
	put("After set_node_fields(File.fields, config.want_input")
    
    --======================================================================
    -- Check that num fields is the same on every line. This is not just a check,
    -- it also an optimization as it ensures that we can reuse the same 
    -- variable [File.fields] for every line without clearing it each time
    -- we read a new line in the file
    --======================================================================    
    if line_count > 1 and n ~= File.num_fields  then
        dv_error("line %d has %d fields but line %d has %d fields", line_count, n, line_count - 1, File.num_fields )
        return false
    end    
    
    -- uptate num_fields for next read
    File.num_fields  = n
    
    return true
end

