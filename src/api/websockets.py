from typing import Dict, Set
from fastapi import WebSocket, WebSocketDisconnect
from fastapi.routing import APIRouter

router = APIRouter()


# WebSocket connection manager to handle multiple client connections
class ConnectionManager:
    def __init__(self):
        # Dictionary to store WebSocket connections by course_id
        self.active_connections: Dict[int, Set[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, course_id: int):
        await websocket.accept()
        if course_id not in self.active_connections:
            self.active_connections[course_id] = set()
        self.active_connections[course_id].add(websocket)

    def disconnect(self, websocket: WebSocket, course_id: int):
        if course_id in self.active_connections:
            self.active_connections[course_id].discard(websocket)
            if not self.active_connections[course_id]:
                del self.active_connections[course_id]

    async def send_item_update(self, course_id: int, item_data: Dict):
        if course_id in self.active_connections:
            disconnected_websockets = set()
            for websocket in self.active_connections[course_id]:
                try:
                    await websocket.send_json(item_data)
                except Exception as exception:
                    print(exception)

                    # Mark for removal if sending fails
                    disconnected_websockets.add(websocket)

            # Remove disconnected websockets
            for websocket in disconnected_websockets:
                self.disconnect(websocket, course_id)


# Create a connection manager instance
manager = ConnectionManager()


# WebSocket endpoint for course generation updates
@router.websocket("/course/{course_id}/generation")
async def websocket_course_generation(websocket: WebSocket, course_id: int):
    try:
        await manager.connect(websocket, course_id)

        # Keep the connection alive until client disconnects
        while True:
            # Wait for any message from the client to detect disconnection
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket, course_id)


# Function to get the connection manager instance
def get_manager() -> ConnectionManager:
    return manager
