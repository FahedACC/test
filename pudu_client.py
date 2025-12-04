# pudu_client.py
import base64
import datetime
import hashlib
import hmac
import json
from typing import Any, Dict, Optional
from urllib.parse import urlparse, parse_qs, unquote

import httpx


class PuduClient:
    """
    Minimal Pudu Cloud client with HMAC-SHA1 auth.

    - Uses application-level credentials (ApiAppKey / ApiAppSecret)
    - Signs each request with HMAC-SHA1 per Pudu Open Platform spec
    - Exposes convenience methods for all endpoints used in the
      Trash-Route Cleaning / BellaBot use case.
    """

    def __init__(self, api_key: str, api_secret: str, base_url: str) -> None:
        if not api_key or not api_secret or not base_url:
            raise ValueError("PuduClient requires api_key, api_secret and base_url")

        # Example base_url (must include /pudu-entry):
        #   https://csg-open-platform.pudutech.com/pudu-entry
        #   https://open-platform.pudutech.com/pudu-entry
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")

        # Reuse a single async client
        self._client = httpx.AsyncClient(timeout=10.0)

    async def close(self) -> None:
        """Close underlying HTTP client (called on FastAPI shutdown)."""
        await self._client.aclose()

    # ------------------------------------------------------------------
    # PUBLIC API METHODS – HEALTH
    # ------------------------------------------------------------------

    async def health_check(self) -> Dict[str, Any]:
        """
        Call Pudu Cloud health check endpoint.

        Adjust `path` if your tenant uses a different health endpoint.
        """
        path = "/data-open-platform-service/v1/api/healthCheck"
        url = f"{self.base_url}{path}"
        return await self._request("GET", url)

    # ------------------------------------------------------------------
    # PUBLIC API METHODS – ROBOTS
    # ------------------------------------------------------------------

    async def list_robot_groups(
        self,
        device: Optional[str] = None,
        shop_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        GET /open-platform-service/v1/robot/group/list

        List robot groups for a given device/shop.
        """
        path = "/open-platform-service/v1/robot/group/list"
        qs_parts = []
        if device:
            qs_parts.append(f"device={device}")
        if shop_id:
            qs_parts.append(f"shop_id={shop_id}")
        query = "&".join(qs_parts)
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{query}"
        return await self._request("GET", url)

    async def list_robots_by_device_and_group(
        self,
        device: str,
        group_id: str,
    ) -> Dict[str, Any]:
        """
        GET /open-platform-service/v1/robot/list_by_device_and_group

        List robots (SN, name, etc.) in a specific group.
        """
        path = "/open-platform-service/v1/robot/list_by_device_and_group"
        query = f"device={device}&group_id={group_id}"
        url = f"{self.base_url}{path}?{query}"
        return await self._request("GET", url)

    # ------------------------------------------------------------------
    # PUBLIC API METHODS – MAPS
    # ------------------------------------------------------------------

    async def list_maps(self, sn: str) -> Dict[str, Any]:
        """
        GET /map-service/v1/open/list

        Returns the list of maps available on the robot.
        """
        path = "/map-service/v1/open/list"
        url = f"{self.base_url}{path}?sn={sn}"
        return await self._request("GET", url)

    async def get_current_map(self, sn: str) -> Dict[str, Any]:
        """
        GET /map-service/v1/open/current

        Returns the current map used by the robot.
        """
        path = "/map-service/v1/open/current"
        url = f"{self.base_url}{path}?sn={sn}&need_element=true"

        return await self._request("GET", url)

    async def list_points(self, sn: str) -> Dict[str, Any]:
        """
        GET /map-service/v1/open/point

        Returns the list of named map points (tables, return points, waypoints, etc.)
        for the current map.
        """
        path = "/map-service/v1/open/point"
        url = f"{self.base_url}{path}?sn={sn}"
        return await self._request("GET", url)

    # ------------------------------------------------------------------
    # PUBLIC API METHODS – MISSIONS (TASKS)
    # ------------------------------------------------------------------

    async def create_task_errand(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST /open-platform-service/v1/task_errand

        Dispatch one or more errands to a robot.

        `body` must follow Pudu's schema:
          {
            "sn": "ROBOT_SN",
            "payload": {
              "tasks": [
                {
                  "task_name": "...",
                  "task_desc": "...",
                  "point_list": [
                    { "map_name": "...", "point": "..." },
                    ...
                  ]
                }
              ]
            }
          }
        """
        path = "/open-platform-service/v1/task_errand"
        url = f"{self.base_url}{path}"
        return await self._request("POST", url, body)

    async def create_transport_task(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST /open-platform-service/v1/transport_task

        Creates an advanced transport/delivery task:
        - start point
        - multiple destinations
        - priority, reminders, etc.

        `body` must follow Pudu's schema:
          {
            "sn": "ROBOT_SN",
            "payload": { ... }
          }
        """
        path = "/open-platform-service/v1/transport_task"
        url = f"{self.base_url}{path}"
        return await self._request("POST", url, body)

    async def custom_call(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST /open-platform-service/v1/custom_call

        Call the robot to a specific map point and optionally display content
        on the screen (image, QR, video, confirmation, etc.).

        Typical body:
          {
            "sn": "ROBOT_SN",
            "map_name": "...",
            "point": "...",
            "call_device_name": "TRASH_ROUTE_BACKEND",
            "call_mode": "CALL",
            "mode_data": { ... }
          }
        """
        path = "/open-platform-service/v1/custom_call"
        url = f"{self.base_url}{path}"
        return await self._request("POST", url, body)

    async def custom_call_cancel(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST /open-platform-service/v1/custom_call/cancel

        Cancel an ongoing custom_call task.

        Body:
          {
            "task_id": "...",
            "call_device_name": "TRASH_ROUTE_BACKEND"
          }
        """
        path = "/open-platform-service/v1/custom_call/cancel"
        url = f"{self.base_url}{path}"
        return await self._request("POST", url, body)

    async def custom_call_complete(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST /open-platform-service/v1/custom_call/complete

        Mark a custom_call as complete, and optionally define `next_call_task`
        to chain another call.

        Body:
          {
            "task_id": "...",
            "call_device_name": "TRASH_ROUTE_BACKEND",
            "next_call_task": { ... }   # optional
          }
        """
        path = "/open-platform-service/v1/custom_call/complete"
        url = f"{self.base_url}{path}"
        return await self._request("POST", url, body)

    # ------------------------------------------------------------------
    # PUBLIC API METHODS – STATUS & POSITION
    # ------------------------------------------------------------------

    async def get_status_by_sn(self, sn: str) -> Dict[str, Any]:
        """
        GET /open-platform-service/v2/status/get_by_sn

        Detailed state for one robot (online, battery, run_state, etc.).
        """
        path = "/open-platform-service/v2/status/get_by_sn"
        url = f"{self.base_url}{path}?sn={sn}"
        return await self._request("GET", url)

    async def get_status_by_group_id(self, group_id: str) -> Dict[str, Any]:
        """
        GET /open-platform-service/v1/status/get_by_group_id

        Status + position (x, y, yaw) for each robot in a group.
        """
        path = "/open-platform-service/v1/status/get_by_group_id"
        url = f"{self.base_url}{path}?group_id={group_id}"
        return await self._request("GET", url)

    async def position_command(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST /open-platform-service/v1/position_command

        Ask a robot to report its position periodically.

        Body:
          {
            "sn": "ROBOT_SN",
            "payload": {
              "interval": 5,
              "times": 100,
              "source": "openAPI"   # optionnel, ignoré si non utilisé par Pudu
            }
          }
        """
        path = "/open-platform-service/v1/position_command"
        url = f"{self.base_url}{path}"
        return await self._request("POST", url, body)

    async def get_robot_task_state(self, sn: str) -> Dict[str, Any]:
        """
        GET /open-platform-service/v1/robot/task/state/get

        Returns list of current tasks running on the robot and their states.
        """
        path = "/open-platform-service/v1/robot/task/state/get"
        url = f"{self.base_url}{path}?sn={sn}"
        return await self._request("GET", url)

    # ------------------------------------------------------------------
    # PUBLIC API METHODS – RECHARGE
    # ------------------------------------------------------------------

    async def recharge_v1(self, sn: str) -> Dict[str, Any]:
        """
        GET /open-platform-service/v1/recharge

        Send robot to its charging/home point via Cloud.
        """
        path = "/open-platform-service/v1/recharge"
        url = f"{self.base_url}{path}?sn={sn}"
        return await self._request("GET", url)

    async def recharge_v2(self, sn: str) -> Dict[str, Any]:
        """
        GET /open-platform-service/v2/recharge

        Same as v1 but uses MQTT connectivity and returns more detailed errors.
        """
        path = "/open-platform-service/v2/recharge"
        url = f"{self.base_url}{path}?sn={sn}"
        return await self._request("GET", url)

    # ------------------------------------------------------------------
    # INTERNAL SIGNED REQUEST
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        url: str,
        body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Generic signed request to Pudu Cloud:
        - Builds x-date (GMT)
        - Normalizes path + query
        - Computes HMAC-SHA1 signature
        - Sends request via httpx
        """
        method = method.upper()
        body = body or {}

        url_info = urlparse(url)
        host = url_info.hostname
        path = url_info.path or "/"

        # Pudu docs: ignore environment prefix (/release, /test, /prepub) in the signed path.
        if path.startswith(("/release", "/test", "/prepub")):
            path = "/" + path[1:].split("/", 1)[1]

        # Normalize query string for signing
        if url_info.query:
            normalized_query = self._normalize_query(unquote(url_info.query))
            if normalized_query:
                path = f"{path}?{normalized_query}"

        accept = "application/json"
        content_type = "application/json"
        content_md5 = ""
        body_json = ""

        if method == "POST":
            # Stable JSON encoding (no unnecessary spaces)
            body_json = json.dumps(body, separators=(",", ":"))
            md5_hex = hashlib.md5(body_json.encode("utf-8")).hexdigest()
            content_md5 = base64.b64encode(md5_hex.encode("utf-8")).decode("utf-8")

        # GMT date header
        x_date = datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")

        # Build signing string
        signing_str = "\n".join(
            [
                f"x-date: {x_date}",
                method,
                accept,
                content_type,
                content_md5,
                path,
            ]
        )

        # HMAC-SHA1 signature
        raw_sig = hmac.new(
            self.api_secret.encode("utf-8"),
            msg=signing_str.encode("utf-8"),
            digestmod=hashlib.sha1,
        ).digest()
        sig_b64 = base64.b64encode(raw_sig).decode("utf-8")

        authorization = (
            f'hmac id="{self.api_key}", '
            f'algorithm="hmac-sha1", '
            f'headers="x-date", '
            f'signature="{sig_b64}"'
        )

        headers = {
            "Host": host or "",
            "Accept": accept,
            "Content-Type": content_type,
            "x-date": x_date,
            "Authorization": authorization,
        }

        if method == "GET":
            resp = await self._client.get(url, headers=headers)
        elif method == "POST":
            resp = await self._client.post(url, headers=headers, content=body_json)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise RuntimeError(
                f"Pudu API error {exc.response.status_code}: {exc.response.text}"
            ) from exc

        try:
            return resp.json()
        except ValueError:
            return {"raw": resp.text, "status_code": resp.status_code}

    @staticmethod
    def _normalize_query(query: str) -> str:
        """
        Pudu-style query normalization:
        - parse_qs
        - sort keys
        - join multi-values with comma
        - drop empty values
        - keep "key" for valueless params
        """
        if not query:
            return ""

        query_dict = parse_qs(query, keep_blank_values=True)
        parts = []

        for key in sorted(query_dict.keys()):
            values = [v for v in query_dict[key] if v != ""]
            if values:
                parts.append(f"{key}=" + ",".join(values))
            else:
                parts.append(key)

        return "&".join(parts)
