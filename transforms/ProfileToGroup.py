import base64
import asyncio
import requests
from dataclasses import dataclass
from typing import Optional, List, Any

from maltego_trx.transform import DiscoverableTransform
from maltego_trx.maltego import MaltegoMsg, MaltegoTransform
from settings import app, loop, test_tgscan_token, prod_tgscan_token, tgscan_debug_mode
from extensions import registry

@dataclass
class TelegramGroup:
    username: str
    title: str
    photo_b64: Optional[str] = None
    raw_source_obj: Any = None 

def search_groups_api(username: str) -> List[dict]:
    if tgscan_debug_mode:
        api_key = test_tgscan_token
        url = "https://api.tgdev.io/tgscan/v1/test/search"
    else:
        api_key = prod_tgscan_token
        url = "https://api.tgdev.io/tgscan/v1/search"
    
    headers = {"Api-Key": api_key}
    try:
        r = requests.post(url, 
                          data={"query": username}, headers=headers, timeout=10)
        return r.json().get("result", {}).get("groups", [])
    except Exception:
        return []

async def unified_resolve(api_groups: List[dict]) -> List[TelegramGroup]:
    results = []
    api_map = {g['username']: g for g in api_groups if g.get('username')}
    usernames = list(api_map.keys())

    async with app:
        resolve_tasks = [app.get_chat(u) for u in usernames]
        resolved_chats = await asyncio.gather(*resolve_tasks, return_exceptions=True)

        photo_tasks = []
        for chat in resolved_chats:
            if not isinstance(chat, Exception) and chat.photo:
                photo_tasks.append(app.download_media(chat.photo.small_file_id, in_memory=True))
            else:
                photo_tasks.append(asyncio.sleep(0)) 

        photos_data = await asyncio.gather(*photo_tasks)

        for i, u_name in enumerate(usernames):
            chat_obj = resolved_chats[i]
            api_info = api_map[u_name]
            
            if not isinstance(chat_obj, Exception):
                p_data = photos_data[i]
                b64 = base64.b64encode(bytes(p_data.getbuffer())).decode("utf-8") if p_data and hasattr(p_data, 'getbuffer') else None
                
                results.append(TelegramGroup(
                    username=chat_obj.username or u_name,
                    title=chat_obj.title,
                    photo_b64=b64,
                    raw_source_obj=chat_obj
                ))
            else:
                results.append(TelegramGroup(
                    username=u_name,
                    title=api_info.get("title", u_name),
                    raw_source_obj=api_info
                ))
                
    return results

@registry.register_transform(
    display_name="To Groups",
    input_entity="interlinked.telegram.UserProfile",
    description="This Transform uses TgScan API to get all chats that the user is a member of",
    output_entities="interlinked.telegram.Group",
)
class ProfileToGroup(DiscoverableTransform):
    @classmethod
    def create_entities(cls, request: MaltegoMsg, response: MaltegoTransform):
        username = request.getProperty("properties.username")
        if not username:
            return

        api_groups = search_groups_api(username)[:5]

        final_groups = loop.run_until_complete(unified_resolve(api_groups))

        for group in final_groups:
            entity = response.addEntity("interlinked.telegram.Group", group.username)
            entity.addProperty("properties.title", "Title", "loose", group.title)
            
            if group.photo_b64:
                entity.addProperty("base64", "Photo", "loose", group.photo_b64)

            source = group.raw_source_obj
            attrs = source.items() if isinstance(source, dict) else vars(source).items()
            
            for attr, value in attrs:
                if value is not None and not str(attr).startswith('_'): 
                    entity.addProperty(attr, str(attr).capitalize(), "loose", str(value))