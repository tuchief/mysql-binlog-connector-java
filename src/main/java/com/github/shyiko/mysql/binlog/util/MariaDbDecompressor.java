package com.github.shyiko.mysql.binlog.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.logging.Logger;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * MariaDB 压缩 binlog payload 解压工具。
 *
 * <h2>压缩 payload 格式（来自 MariaDB 源码 sql/log_event.h MDEV-11065）</h2>
 * <pre>
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │ Compressed Record                                                   │
 * ├──────────┬──────────────────────────────────────────────────────── │
 * │ Byte 0   │ Header（1字节）                                          │
 * │          │   bit 7   : 1 = 已压缩                                   │
 * │          │   bit 4-6 : 保留（算法标识，当前始终为 0 = zlib）          │
 * │          │   bit 0-3 : lenlen，原始长度字段所占字节数（1-4）          │
 * ├──────────┼──────────────────────────────────────────────────────── │
 * │ Byte 1.. │ 原始数据长度（lenlen 字节，小端序 Little-Endian）          │
 * ├──────────┼──────────────────────────────────────────────────────── │
 * │ 剩余部分 │ zlib DEFLATE 压缩数据                                     │
 * └──────────┴──────────────────────────────────────────────────────── │
 * </pre>
 *
 * <h2>关键点</h2>
 * <ul>
 *   <li>对于 QUERY_COMPRESSED_EVENT：压缩 payload 是事件 body 中
 *       跳过 post-header + status_vars + db 字段之后的剩余部分（SQL文本）。</li>
 *   <li>对于 *_ROWS_COMPRESSED_EVENT：压缩 payload 是行数据 body 中
 *       跳过 table_id + flags + extra_data + column_count + column_bitmap
 *       之后的剩余部分（rows data）。</li>
 *   <li>两种场景调用同一个解压方法 {@link #decompress(byte[], int, int)}。</li>
 * </ul>
 */
public final class MariaDbDecompressor {

    private static final Logger log = Logger.getLogger(MariaDbDecompressor.class.getName());

    private MariaDbDecompressor() {}

    /**
     * 解压 MariaDB binlog 压缩 payload。
     *
     * @param src    包含压缩 payload 的字节数组
     * @param offset src 中压缩 payload 的起始偏移
     * @param length 压缩 payload 的字节长度
     * @return 解压后的原始字节数组
     * @throws IOException 解压失败或数据格式错误
     */
    public static byte[] decompress(byte[] src, int offset, int length) throws IOException {
        if (length < 1) {
            throw new IOException("Compressed payload too short: " + length + " bytes");
        }

        // ✅ 用 int 避免 byte 符号扩展
        int header = src[offset] & 0xFF;

        // bit7 = 0 → 未压缩（未达到 log_bin_compress_min_len）
        if ((header & 0x80) == 0) {
            byte[] raw = new byte[length - 1];
            System.arraycopy(src, offset + 1, raw, 0, raw.length);
            return raw;
        }

        // bits 4-6 = 算法（始终为 0 = zlib）
        int algorithm = (header >> 4) & 0x07;
        if (algorithm != 0) {
            throw new IOException(
                "Unsupported compression algorithm: " + algorithm
                    + " (only zlib=0 is supported)");
        }

        // bits 0-2 = lenlen（原始长度字段的字节数，0-4）
        // ✅ 0x0F → 0x07（3位，MariaDB 源码标准）
        int lenlen = header & 0x07;
        if (lenlen > 4) {
            throw new IOException(
                "Invalid lenlen in compressed header: " + lenlen + " (must be 0-4)");
        }
        if (length < 1 + lenlen) {
            throw new IOException(
                "Compressed payload truncated: header claims lenlen=" + lenlen
                    + " but only " + (length - 1) + " bytes remain");
        }

        int dataOffset    = offset + 1 + lenlen;
        int compressedLen = length - 1 - lenlen;

        // 读取原始数据长度（小端序） ❌错误的
        // int originalLen = 0;
        // for (int i = 0; i < lenlen; i++) {
        //     originalLen |= (src[offset + 1 + i] & 0xFF) << (8 * i);
        // }

        // ✅ 大端序读取（MariaDB MyISAM pack format = big-endian）
        // ✅ lenlen=0 时 originalLen=0，走动态 inflate 分支，不抛异常
        int originalLen = 0;
        for (int i = 0; i < lenlen; i++) {
            originalLen = (originalLen << 8) | (src[offset + 1 + i] & 0xFF);
        }

        if (originalLen > 128 * 1024 * 1024) {
            throw new IOException(
                "Suspicious original length in compressed header: " + originalLen);
        }

        Inflater inflater = new Inflater();
        try {
            inflater.setInput(src, dataOffset, compressedLen);

            if (originalLen > 0) {
                // 已知原始长度：固定大小缓冲区 inflate
                byte[] output = new byte[originalLen];
                int total = 0;
                while (total < originalLen && !inflater.finished()) {
                    int count;
                    try {
                        count = inflater.inflate(output, total, originalLen - total);
                    } catch (DataFormatException e) {
                        throw new IOException("zlib decompression failed: " + e.getMessage(), e);
                    }
                    if (count == 0 && inflater.needsInput()) break;
                    total += count;
                }
                if (total != originalLen) {
                    throw new IOException(
                        "Decompressed length mismatch: expected=" + originalLen
                            + ", actual=" + total);
                }
                // ✅ finished()=false 只打 warn，不抛异常（trailing checksum 等情况）
                if (!inflater.finished()) {
                    log.warning("zlib stream not finished after reading expected " + originalLen +" bytes, "
                        + "possible trailing data (ignored)");
                }
                return output;

            } else {
                // lenlen=0：无原始长度字段，动态扩展 inflate
                ByteArrayOutputStream baos = new ByteArrayOutputStream(compressedLen * 3);
                byte[] chunk = new byte[8192];
                while (!inflater.finished()) {
                    int count;
                    try {
                        count = inflater.inflate(chunk);
                    } catch (DataFormatException e) {
                        throw new IOException("zlib decompression failed: " + e.getMessage(), e);
                    }
                    if (count == 0 && inflater.needsInput()) break;
                    baos.write(chunk, 0, count);
                }
                return baos.toByteArray();
            }
        } finally {
            inflater.end();
        }
    }

    /**
     * 便捷方法：解压整个字节数组（从偏移 0 开始）。
     *
     * @param src 压缩 payload 字节数组
     * @return 解压后的原始字节数组
     * @throws IOException 解压失败
     */
    public static byte[] decompress(byte[] src) throws IOException {
        return decompress(src, 0, src.length);
    }
}
