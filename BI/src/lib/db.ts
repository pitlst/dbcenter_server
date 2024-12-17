// lib/db.ts
import mongoose from 'mongoose';

const MongoDB_URI: string = process.env.MONGODB_URI || 'mongo::';

// 连接到MongoDB
mongoose.connect(MongoDB_URI, {});

// 打印错误信息
mongoose.connection.on('error', (error) => {
    console.error('MongoDB connection error:', error);
});

export default mongoose;