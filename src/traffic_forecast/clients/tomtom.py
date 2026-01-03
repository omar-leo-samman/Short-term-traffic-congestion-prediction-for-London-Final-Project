"""TomTom Traffic API client (Flow Segment Data)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from ..http import ApiError, HttpClient, JsonType, request_json


@dataclass(frozen=True)
class FlowSegmentMetrics:
    """Convenient parsed fields from TomTom Flow Segment Data."""

    frc: Optional[str]
    current_speed: Optional[float]
    free_flow_speed: Optional[float]
    current_travel_time: Optional[float]
    free_flow_travel_time: Optional[float]
    confidence: Optional[float]
    road_closure: Optional[bool]
    congestion_index: Optional[float]


class TomTomClient:
    """Minimal TomTom Traffic Flow client."""

    def __init__(self, *, api_key: str, http: HttpClient, base_url: str = "https://api.tomtom.com") -> None:
        self.api_key = api_key
        self.http = http
        self.base_url = base_url.rstrip("/")

    def flow_segment_data(
        self,
        *,
        lat: float,
        lon: float,
        style: str = "absolute",
        zoom: int = 10,
        unit: str = "kmph",
        openlr: bool = False,
    ) -> Dict[str, Any]:
        """Call Flow Segment Data (JSON).

        Docs URL format (v4):
        /traffic/services/4/flowSegmentData/{style}/{zoom}/json?key=...&point=lat,lon
        """

        url = f"{self.base_url}/traffic/services/4/flowSegmentData/{style}/{zoom}/json"
        params = {
            "key": self.api_key,
            "point": f"{lat},{lon}",
            "unit": unit,
        }
        if openlr:
            params["openLr"] = "true"

        data = request_json(self.http, url, params=params)
        if not isinstance(data, dict):
            raise ApiError(f"Unexpected TomTom response type: {type(data)}")
        return data

    @staticmethod
    def parse_metrics(payload: Dict[str, Any]) -> FlowSegmentMetrics:
        """Extract key metrics and compute a simple congestion index."""

        fsd = payload.get("flowSegmentData", {})
        if not isinstance(fsd, dict):
            fsd = {}

        current_speed = fsd.get("currentSpeed")
        free_flow_speed = fsd.get("freeFlowSpeed")

        congestion_index: Optional[float] = None
        try:
            if current_speed is not None and free_flow_speed:
                congestion_index = 1.0 - (float(current_speed) / float(free_flow_speed))
                # Clip to [0, 1] for stability
                congestion_index = max(0.0, min(1.0, congestion_index))
        except Exception:
            congestion_index = None

        return FlowSegmentMetrics(
            frc=fsd.get("frc"),
            current_speed=float(current_speed) if current_speed is not None else None,
            free_flow_speed=float(free_flow_speed) if free_flow_speed is not None else None,
            current_travel_time=float(fsd.get("currentTravelTime")) if fsd.get("currentTravelTime") is not None else None,
            free_flow_travel_time=float(fsd.get("freeFlowTravelTime")) if fsd.get("freeFlowTravelTime") is not None else None,
            confidence=float(fsd.get("confidence")) if fsd.get("confidence") is not None else None,
            road_closure=bool(fsd.get("roadClosure")) if fsd.get("roadClosure") is not None else None,
            congestion_index=congestion_index,
        )
