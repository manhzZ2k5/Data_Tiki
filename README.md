# Data_Tiki
Dự án Python thu thập dữ liệu 200,000+ sản phẩm từ Tiki API. Hỗ trợ lưu dữ liệu dạng JSON, xử lý theo batch, tự động lưu checkpoint khi gặp lỗi.
MÔ TẢ:
  Chia id thành các batch sau đó dùng concurrent.futures để chạy multithread để tối ưu thời gian
  Lưu lại trạng thái hoặc tiến độ hiện tại vào Checkpoint trong quá trình chạy, để nếu bị dừng hoặc gặp lỗi thì có thể tiếp tục từ điểm đã lưu thay vì phải chạy lại   từ đầu.
KẾT QUẢ:
  Tổng số ID : 200,000
  Tổng số ID success: 198,939
  Tổng số ID loi: 1,061
  Số batch đã hoàn thành: 200
  Tỷ lệ thành công: 99.47%
  Tỷ lệ i :0.53%
