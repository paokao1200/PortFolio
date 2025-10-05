function IDA.main()
    if not IDA.m then
        local err = ""
        --IDA.m, err = mqtt.create("tcp://mqtt.lattapol.com:1883", "ClienID")  -- create connection
        if IDA.m then
            IDA.config = {
                username = "4kYBNqTa85bEE54XMHscmEsrMkhDknR5",
                password = "cyGKcyP2yANs6Nb3F6A1dm4kXCQNT45y",
                netway = 1, -- Ethernet connection, WIFI=1
                -- keepalive = 100, -- Optional, set the connection heartbeat interval for 100 seconds.
                -- cleansession = 0, -- Optional, keep session
            }
            IDA.m:on("message", function(topic, msg) -- Register for receiving message callbacks
                local str = string.format("%s:%s", topic, msg)
                print("mqtt msg:", str) -- Print out the received topics and content
            end)
            IDA.m:on("offline", function (cause) -- Register for lost connection callbacks
                -- addr_setstring("@xxx", "cause"..(cause or " got nil"))
            end)
            IDA.m:on("arrived", function() -- Registration for sending messages to callbacks 
                print("msg arrived")
            end)
        else
            print("mqtt create failed:", err) -- Create object failed
        end
    else
        if IDA.m:isconnected() then
            print("Energy : ",addr_getdword("@ACT_ENERGY_RW"))
			-- Consumption
			local Consumption_Data = {
			    data = {
			        Main_Total=tonumber(string.format("%.2f",addr_getfloat("@MDB-001_Active Energy Delivered",1,2))),
    				Supply_Pump_ABC=tonumber(string.format("%.2f",addr_getfloat("@SupplyPumpABC",1,2))),
					Transfer_Pump_ABC=tonumber(string.format("%.2f",addr_getfloat("@TransferPumpABC",1,2))),
					Other=tonumber(string.format("%.2f",addr_getfloat("@Other",1,2)))
    				}
			}
			IDA.m:publish("@shadow/data/update", json.encode(Consumption_Data) , 0, 0)
			-- MDB-001
				local MDB_Data = {
			    data = {
			        MDB001_Energy=tonumber(string.format("%.2f",addr_getfloat("@MDB-001_Active Energy Delivered",1,2))),
    				MDB001_Voltage=tonumber(string.format("%.2f",addr_getfloat("@MDB-001_Voltage L-L Avg",1,2))),
					MDB001_Current=tonumber(string.format("%.2f",addr_getfloat("@MDB-001_Current Avg",1,2))),
					MDB001_Power_Factor=tonumber(string.format("%.2f",addr_getfloat("@MDB-001_Power Factor Total",1,2)))
    				}
			}
			IDA.m:publish("@shadow/data/update", json.encode(MDB_Data) , 0, 0)
			-- CWP
				local CWP_Data = {
			    data = {
			        CWPA_Energy=tonumber(string.format("%.2f",addr_getfloat("@CWP-A_Active Energy Delivered",1,2))),
    				CWPA_Voltage=tonumber(string.format("%.2f",addr_getfloat("@CWP-A_Voltage L-L Avg",1,2))),
					CWPA_Current=tonumber(string.format("%.2f",addr_getfloat("@CWP-A_Current Avg",1,2))),
					CWPA_Power_Factor=tonumber(string.format("%.2f",addr_getfloat("@CWP-A_Power Factor Total",1,2))),
					CWPB_Energy=tonumber(string.format("%.2f",addr_getfloat("@CWP-B_Active Energy Delivered",1,2))),
    				CWPB_Voltage=tonumber(string.format("%.2f",addr_getfloat("@CWP-B_Voltage L-L Avg",1,2))),
					CWPB_Current=tonumber(string.format("%.2f",addr_getfloat("@CWP-B_Current Avg",1,2))),
					CWPB_Power_Factor=tonumber(string.format("%.2f",addr_getfloat("@CWP-B_Power Factor Total",1,2))),
					CWPC_Energy=tonumber(string.format("%.2f",addr_getfloat("@CWP-C_Active Energy Delivered",1,2))),
    				CWPC_Voltage=tonumber(string.format("%.2f",addr_getfloat("@CWP-C_Voltage L-L Avg",1,2))),
					CWPC_Current=tonumber(string.format("%.2f",addr_getfloat("@CWP-C_Current Avg",1,2))),
					CWPC_Power_Factor=tonumber(string.format("%.2f",addr_getfloat("@CWP-C_Power Factor Total",1,2)))
    				}
			}
			IDA.m:publish("@shadow/data/update", json.encode(CWP_Data) , 0, 0)
			-- TP
				local TP_Data = {
			    data = {
			        TPA_Energy=tonumber(string.format("%.2f",addr_getfloat("@TP-A_Active Energy Delivered",1,2))),
    				TPA_Voltage=tonumber(string.format("%.2f",addr_getfloat("@TP-A_Voltage L-L Avg",1,2))),
					TPA_Current=tonumber(string.format("%.2f",addr_getfloat("@TP-A_Current Avg",1,2))),
					TPA_Power_Factor=tonumber(string.format("%.2f",addr_getfloat("@TP-A_Power Factor Total",1,2))),
					TPB_Energy=tonumber(string.format("%.2f",addr_getfloat("@TP-B_Active Energy Delivered",1,2))),
    				TPB_Voltage=tonumber(string.format("%.2f",addr_getfloat("@TP-B_Voltage L-L Avg",1,2))),
					TPB_Current=tonumber(string.format("%.2f",addr_getfloat("@TP-B_Current Avg",1,2))),
					TPB_Power_Factor=tonumber(string.format("%.2f",addr_getfloat("@TP-B_Power Factor Total",1,2))),
					TPC_Energy=tonumber(string.format("%.2f",addr_getfloat("@TP-C_Active Energy Delivered",1,2))),
    				TPC_Voltage=tonumber(string.format("%.2f",addr_getfloat("@TP-C_Voltage L-L Avg",1,2))),
					TPC_Current=tonumber(string.format("%.2f",addr_getfloat("@TP-C_Current Avg",1,2))),
					TPC_Power_Factor=tonumber(string.format("%.2f",addr_getfloat("@TP-C_Power Factor Total",1,2)))
    				}
			}
			IDA.m:publish("@shadow/data/update", json.encode(TP_Data) , 0, 0)
		else
            local stat, err = IDA.m:connect(IDA.config) -- connection
            if stat == nil then --Determine whether to connect
                print("mqtt connect failed:", err)
                return -- Connection failed, return directly
            end
        end
    end
end