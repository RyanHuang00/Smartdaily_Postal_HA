# Smartdaily\_Postal\_HA · RyanHuang00 fork

> **這是 [`andyching168/Smartdaily_Postal_HA`](https://github.com/andyching168/Smartdaily_Postal_HA) 的個人 fork**，原作所有功能 + 文件保留不動。本 fork 多出下列能力：
>
> - **`sensor.bao_guo_li_shi`**：暴露 `coordinator.data["all_packages"]` 為 attribute（最近 30 筆含已領 + 未領 + 本機照片路徑）。
> - **照片自動歸檔**：每次 poll 把新 `postal_img` 下載到 `/config/www/packages/<pd_id>.jpg`，永久保存（規避上游 GCS signed URL 15 分鐘過期）。
> - **新事件 `smartdaily_postal_ha_new_package`**：每當 poll 偵測到新 `pd_id` 即 fire，event payload 為完整 package dict。
> - **新事件 `smartdaily_postal_ha_package_picked_up`**：每當 `p_status` 從 `1` 轉 `2` 即 fire，event payload 含 `previous_status`、`new_status`。
> - HA 重啟後第一次 poll 不 fire 任何事件，避免歷史包裹被誤判為「新到」。
>
> 詳見 commit `feat: history sensor + photo archive + new_package / package_picked_up events`。
>
> 上游沒有 LICENSE 檔，本 fork 也不另行授權；僅做個人使用。

---

### 將今網智生活的包裹領取狀態串接到Home Assistant的工具

## 安裝

### 前提條件

- Home Assistant安裝。
- HACS (Home Assistant Community Store) 安裝。

### 取得DeviceSn (也就是裝置識別)

為了使用這個組件，你需要取得DeviceSn。
別擔心，這非常好取得，可依照以下步驟

1. 在智生活APP首頁，點擊右上角的「條碼」（也就是領取包裹時給管理員掃描的頁面）
2. 將此頁面截圖
3. 到[條碼掃瞄網站](https://online-barcode-reader.inliteresearch.com/)，將截圖上傳到網站辨識。
4. 複製辨識出來的字串（也就是`DeviceSn(或DeviceCode)`的值）。

### 通過HACS安裝

1. 在HACS中，選擇“Integrations”。
2. 點擊右上角的選單按鈕，選擇`Custom repositories`，將此repo貼上，類型選擇`Integration`。
3. 搜索“智生活包裹追蹤”並安裝。

### 配置Home Assistant

1. 重新啟動您的Home Assistant。
2. 在Home Assistant的“配置” > “整合”頁面上，點擊“添加集成”。
3. 搜索“智生活包裹追蹤”並選擇它。
4. 在出現的窗口中，輸入先前辨識出來的`DeviceSn(或DeviceCode)`值。
5. 點擊“提交”，並選擇自己的社區，完成設置。

## 使用

一旦完成安裝和配置，您將可以在Home Assistant中看到一個新的感應器，顯示您的包裹追蹤信息。

### 額外配置查看寄放物品詳情

如果您想查看寄放物的詳細信息，請按照以下步驟進行設置：

1. 下載在collection資料夾內的`collection_fetch.py`，編輯 `collection_fetch.py` 文件中的 `DeviceID` ，將其設置為您的裝置ID。

2. 將編輯好的 Python 腳本 (`collection_fetch.py`) 上傳到 Home Assistant 的配置資料夾（通常是 `/config` 或 `/homeassistant`）。

3. 在 Home Assistant 的 `configuration.yaml` 文件中添加以下 Command Line Sensor 設置：

   ```yaml
   command_line:
      - sensor:
            name: "最新寄放物狀態"
            command: "python /config/collection_fetch.py"
            value_template: "{{ value_json.latest.status }}"
            json_attributes_path: "$.latest"
            json_attributes:
               - serial_num
               - date
               - from_name
               - to_name
               - from_tablet
               - to_tablet
               - c_dtype
               - c_money
               - sdate
               - ddate
               - note
               - collection_image
               - uncollected_count
            scan_interval: 300
      - sensor:
            name: "已領取寄放物狀態"
            command: "python /config/collection_fetch.py"
            value_template: "{{ value_json.collected.ddate }}"
            json_attributes_path: "$.collected"
            json_attributes:
               - serial_num
               - date
               - from_name
               - to_name
               - from_tablet
               - to_tablet
               - c_dtype
               - c_money
               - sdate
               - ddate
               - note
               - collection_image
            scan_interval: 300

   ```

4. **（可選）新增寄放物 Slot 1~4 感測器**：如果您想同時顯示多筆未領取的寄放物，可以額外新增以下設定：

   ```yaml
   command_line:
      - sensor:
            name: "Collection 1"
            command: "python /config/collection_fetch.py"
            value_template: "{{ value_json.slot_1.status }}"
            json_attributes_path: "$.slot_1"
            json_attributes:
               - has_item
               - serial_num
               - date
               - from_name
               - to_name
               - from_tablet
               - to_tablet
               - c_dtype
               - c_money
               - sdate
               - ddate
               - note
               - collection_image
            scan_interval: 300
      - sensor:
            name: "Collection 2"
            command: "python /config/collection_fetch.py"
            value_template: "{{ value_json.slot_2.status }}"
            json_attributes_path: "$.slot_2"
            json_attributes:
               - has_item
               - serial_num
               - date
               - from_name
               - to_name
               - from_tablet
               - to_tablet
               - c_dtype
               - c_money
               - sdate
               - ddate
               - note
               - collection_image
            scan_interval: 300
      - sensor:
            name: "Collection 3"
            command: "python /config/collection_fetch.py"
            value_template: "{{ value_json.slot_3.status }}"
            json_attributes_path: "$.slot_3"
            json_attributes:
               - has_item
               - serial_num
               - date
               - from_name
               - to_name
               - from_tablet
               - to_tablet
               - c_dtype
               - c_money
               - sdate
               - ddate
               - note
               - collection_image
            scan_interval: 300
      - sensor:
            name: "Collection 4"
            command: "python /config/collection_fetch.py"
            value_template: "{{ value_json.slot_4.status }}"
            json_attributes_path: "$.slot_4"
            json_attributes:
               - has_item
               - serial_num
               - date
               - from_name
               - to_name
               - from_tablet
               - to_tablet
               - c_dtype
               - c_money
               - sdate
               - ddate
               - note
               - collection_image
            scan_interval: 300
   ```

   > 💡 **提示**：每個 Slot 的 `has_item` 屬性可用於判斷該位置是否有寄放物，方便在 Lovelace 中做條件顯示。

5. 為了顯示寄放物的圖片，您可以在 Home Assistant 中配置 Template Image：

   ```yaml
   {{ state_attr("sensor.zui_xin_ji_fang_wu_zhuang_tai", "collection_image") }}
   ```
   ```yaml
   {{ state_attr("sensor.yi_ling_qu_ji_fang_wu_zhuang_tai", "collection_image") }}
   ```
這樣，您就可以在 Home Assistant 中查看最新的寄放物狀態、最後領取的寄放物狀態以及相關的圖片等信息。

### 額外配置查看退貨包裹

若您的社區有啟用退貨功能，可用單獨腳本查詢：

1. 下載 `collection` 資料夾內的 `return_postal_fetch.py`，編輯檔案中的 `DeviceID` 與 `ComID`（社區ID）。
   - `DeviceID` 取得方式同前述條碼掃描步驟。
   - `ComID` 可用 `tool/API_Test/main.py` 查詢，或在整合完成後從 Home Assistant 實體屬性取得。
2. 將編輯好的 `return_postal_fetch.py` 上傳到 Home Assistant 的配置資料夾（通常是 `/config` 或 `/homeassistant`）。
3. 在 `configuration.yaml` 中新增 Command Line Sensor：

   ```yaml
   command_line:
      - sensor:
            name: "退貨包裹最新狀態"
            command: "python /config/return_postal_fetch.py"
            value_template: "{{ value_json.latest.status }}"
            json_attributes_path: "$.latest"
            json_attributes:
               - serial_num
               - create_date
               - transport_code
               - postal_type
               - logistics
               - storage
               - image
               - sign_image
               - return_date
               - status_code
            scan_interval: 300
      - sensor:
            name: "退貨包裹數量"
            command: "python /config/return_postal_fetch.py"
            value_template: "{{ value_json.count }}"
            scan_interval: 300
   ```

4. `return_postal_fetch.py` 也會輸出完整 `items` 陣列（前端或自訂卡片可直接取用）。

### 額外配置查看社區公告

如果您想查看社區公告的詳細信息，請按照以下步驟進行設置：

1. 下載在 collection 資料夾內的 `bulletin_fetch.py`，編輯文件中的 `DeviceID` 和 `ComID`（社區ID），將其設置為您的裝置ID和社區ID。

   > 💡 **如何取得社區ID？** 可以使用 `tool/API_Test/main.py` 工具查詢，或在整合設定完成後從 Home Assistant 的實體屬性中取得。

2. 將編輯好的 Python 腳本 (`bulletin_fetch.py`) 上傳到 Home Assistant 的配置資料夾（通常是 `/config` 或 `/homeassistant`）。

3. 在 Home Assistant 的 `configuration.yaml` 文件中添加以下 Command Line Sensor 設置：

   ```yaml
   command_line:
      - sensor:
            name: "社區公告"
            command: "python /config/bulletin_fetch.py"
            value_template: "{{ value_json.latest.title }}"
            json_attributes_path: "$.latest"
            json_attributes:
               - id
               - title
               - start_date
               - end_date
               - content
               - attachments
               - attachment_count
            scan_interval: 3600  # 每小時更新一次
   ```

4. 這樣您就可以在 Home Assistant 中查看最新的社區公告。公告的屬性包含：

   | 屬性 | 說明 |
   |------|------|
   | `title` | 公告標題 |
   | `content` | 公告內容 |
   | `start_date` | 公告發布日期 |
   | `end_date` | 公告結束日期 |
   | `attachments` | 附件連結列表 |
   | `attachment_count` | 附件數量 |

5. 若要在 Lovelace 卡片中顯示公告內容，可以使用 Markdown 卡片：

   ```yaml
   type: markdown
   content: |
     ## {{ state_attr('sensor.she_qu_gong_gao', 'title') }}
     **發布日期：** {{ state_attr('sensor.she_qu_gong_gao', 'start_date') }}
     
     {{ state_attr('sensor.she_qu_gong_gao', 'content') }}
   ```

## 🔐 給今網的 APP 改善建議

針對目前條碼作為身分驗證憑證（DeviceSn）設計風險，以下為不需修改後端 API 的 UX 層改善提案：

- 條碼顯示前，**推薦使用者啟用 Face ID / 指紋驗證**
- App 預設**禁止螢幕截圖與錄影**（保留使用者手動開啟權限）
- 條碼畫面**加入警語提醒**（例如「此條碼具實際查詢權限，請勿隨意截圖或轉傳」）

上述機制能有效抑制條碼外洩的實務風險，**主要是因現有條碼具過大授權權限（可查詢個資與包裹紀錄）所致。**


## 📜 授權 License

本專案採用 [MIT License](LICENSE) 授權，詳情請見 LICENSE 檔案。

## ⚠️ 免責聲明 Disclaimer

本工具為非官方開發，僅供學術研究與個人自動化用途。  
使用本專案所造成之任何資料損失、帳號問題或法律糾紛，開發者不負任何責任。  
若官方對 API 做出限制或變更，本工具功能亦可能失效。

本專案無與今網智生活有任何關聯或合作。
