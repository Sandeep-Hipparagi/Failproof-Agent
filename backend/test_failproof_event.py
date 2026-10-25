"""Emit live session telemetry to Failproof AI Cloud using failproofai_sdk."""
import time
import uuid
import failproofai_sdk as fp

def main():
    print(f"Loaded failproofai_sdk version: {fp.__version__}")
    
    with fp.session(agent_id="restaurant-concierge-bengaluru"):
        print("Emitting session lifecycle events to Failproof Cloud...")
        
        fp.event.agent_start(goal="Book a table at Karavalli for 2 people")
        fp.event.human_input(input_id="inp-001", response="Book a table at Karavalli for 2 at 8 PM tonight")
        
        req_id = "req-" + uuid.uuid4().hex[:8]
        fp.event.model_request(
            model="gemini-2.5-flash",
            request_id=req_id,
            messages=[{"role": "user", "content": "Find availability at Karavalli"}]
        )
        fp.event.model_response(
            model="gemini-2.5-flash",
            request_id=req_id,
            content="Checking availability at Karavalli for 2 people at 8:00 PM...",
            input_tokens=42,
            output_tokens=18
        )
        
        # Tool call
        with fp.tool_call("check_table_availability", input={"restaurant": "Karavalli", "party_size": 2, "time_slot": "20:00"}):
            time.sleep(0.05)
            
        fp.event.tool_result(tool_name="check_table_availability", tool_call_id="call-001", output={"available": True, "hold_token": "TOK-KV-821"})
        
        # Second Tool call (execution with policy gate)
        with fp.tool_call("execute_reservation", input={"place_id": "karavalli_residency_rd", "hold_token": "TOK-KV-821", "party_size": 2}):
            time.sleep(0.05)
            
        fp.event.tool_result(tool_name="execute_reservation", tool_call_id="call-002", output={"status": "CONFIRMED", "reservation_id": "RES-8891"})
        
        fp.event.agent_end(outcome="SUCCESS", summary="Reservation successfully confirmed at Karavalli for 2 guests at 8:00 PM.")

    print("Successfully flushed session telemetry to Failproof AI daemon!")

if __name__ == "__main__":
    main()
