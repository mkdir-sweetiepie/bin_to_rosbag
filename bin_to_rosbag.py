#!/usr/bin/env python3

import os
import sys
import numpy as np
from glob import glob
import rclpy
from rclpy.serialization import serialize_message
from sensor_msgs.msg import PointCloud2, PointField
from std_msgs.msg import Header
from rosbag2_py import SequentialWriter, StorageOptions, ConverterOptions, TopicMetadata

def read_bin_velodyne(path):
    """bin 파일에서 포인트 클라우드 데이터 읽기"""
    points = np.fromfile(path, dtype=np.float32).reshape(-1, 4)
    return points

def create_cloud_message(points, stamp, frame_id):
    """PointCloud2 메시지 생성"""
    msg = PointCloud2()
    msg.header = Header()
    msg.header.stamp = stamp
    msg.header.frame_id = frame_id
    
    msg.height = 1
    msg.width = points.shape[0]
    
    msg.fields = [
        PointField(name='x', offset=0, datatype=PointField.FLOAT32, count=1),
        PointField(name='y', offset=4, datatype=PointField.FLOAT32, count=1),
        PointField(name='z', offset=8, datatype=PointField.FLOAT32, count=1),
        PointField(name='intensity', offset=12, datatype=PointField.FLOAT32, count=1)
    ]
    
    msg.is_bigendian = False
    msg.point_step = 16  # 4 * sizeof(float)
    msg.row_step = msg.point_step * points.shape[0]
    msg.is_dense = True
    msg.data = points.tobytes()
    
    return msg

def convert_bin_to_rosbag(bin_folder, output_bag):
    """bin 파일들을 ROS 2 bag 파일로 변환"""
    # 모든 bin 파일 경로 가져오기
    bin_files = sorted(glob(os.path.join(bin_folder, '*.bin')))
    
    if not bin_files:
        print(f"No bin files found in {bin_folder}")
        return
    
    print(f"Found {len(bin_files)} bin files. Starting conversion...")
    
    # ROS 2 bag 파일 설정
    storage_options = StorageOptions(uri=output_bag, storage_id='sqlite3')
    converter_options = ConverterOptions(
        input_serialization_format='cdr',
        output_serialization_format='cdr'
    )
    
    writer = SequentialWriter()
    writer.open(storage_options, converter_options)
    
    # 토픽 메타데이터 생성
    topic_name = '/points_raw'
    topic_metadata = TopicMetadata(
        name=topic_name,
        type='sensor_msgs/msg/PointCloud2',
        serialization_format='cdr',
        offered_qos_profiles=''
    )
    
    # 토픽 생성
    writer.create_topic(topic_metadata)
    
    # bin 파일을 메시지로 변환하고 bag에 쓰기
    for i, bin_file in enumerate(bin_files):
        # bin 파일 읽기
        points = read_bin_velodyne(bin_file)
        
        # 시간 스탬프 생성 (파일 순서에 따라 초 단위로 증가)
        now = rclpy.time.Time(seconds=i).to_msg()
        
        # PointCloud2 메시지 생성
        cloud_msg = create_cloud_message(points, now, 'velodyne')
        
        # 메시지 직렬화 및 bag에 쓰기
        writer.write(
            topic_name,
            serialize_message(cloud_msg),
            now.sec * 1000000000 + now.nanosec
        )
        
        if (i + 1) % 100 == 0:
            print(f"Processed {i + 1}/{len(bin_files)} files...")
    
    print(f"Conversion complete! Bag file saved to {output_bag}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 bin_to_rosbag.py <bin_folder> <output_bag>")
        sys.exit(1)
    
    bin_folder = sys.argv[1]  # bin 파일이 있는 폴더
    output_bag = sys.argv[2]  # 출력 bag 파일 경로
    
    rclpy.init()
    try:
        convert_bin_to_rosbag(bin_folder, output_bag)
    finally:
        rclpy.shutdown()
